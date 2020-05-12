import json
import time
from enum import Enum

from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


# Possible known states for a Metronome job
class MetronomeJobState(Enum):
    INITIAL = 1
    STARTING = 2
    ACTIVE = 3
    SUCCESS = 200
    FAILED = -1
    UNDEFINED = 666


class MetronomeOperator(BaseOperator):
    """
    Operator for deploying Metronome jobs on DC/OS.

    It does the following:
        - Gets an authorization token from the IAM API using provided credentials.
          The token is used for all following API calls.
        - Upserts the Metronome job:
            - If a job was created before with the same ID, its definition is updated with metronome_job_json
            - Otherwise it inserts a new job with the definition metronome_job_json
        - Starts the upserted job.
        - Monitors the started job by polling its state every 60 seconds.
        - When the job succeeds or fails, the operator terminates as well.

    :param metronome_job_json: The Metronome job definition
    :type metronome_job_json: json
    :param dcos_http_conn_id: The HTTP connection to DC/OS to run the operator against
    :type dcos_http_conn_id: string
    :param dcos_robot_user_name: User name used for authorization
    :type dcos_robot_user_name: string
    :param dcos_robot_user_pwd: Password used for authorization
    :type dcos_robot_user_pwd: string
    :param dcos_iam_api_auth_endpoint: DC/OS IAM endpoint
    :type dcos_iam_api_auth_endpoint: string
    :param dcos_metronome_jobs_api_endpoint: DC/OS Metronome jobs endpoint
    :type dcos_metronome_jobs_api_endpoint: string


    TODO exponential retry for DC/OS API calls
    TODO include {'check_response': True} as an extra_options parameter to the HTTPHook.run() method
    """

    template_fields = ('metronome_job_json',
                       'dcos_http_conn_id',
                       'dcos_robot_user_name',
                       'dcos_robot_user_pwd',
                       'dcos_iam_api_auth_endpoint',
                       'dcos_metronome_jobs_api_endpoint')
    template_ext = ()
    ui_color = '#f4a460'

    SECONDS_TO_SLEEP_BETWEEN_RETRIES = 60

    @apply_defaults
    def __init__(self,
                 metronome_job_json,
                 dcos_http_conn_id,
                 dcos_robot_user_name,
                 dcos_robot_user_pwd,
                 dcos_iam_api_auth_endpoint='/acs/api/v1/auth/login',
                 dcos_metronome_jobs_api_endpoint='/service/metronome/v1/jobs',
                 *args, **kwargs):

        super(MetronomeOperator, self).__init__(*args, **kwargs)

        self.metronome_job_json = metronome_job_json
        # Extract the job ID from the job's definition
        self._metronome_job_id = json.loads(metronome_job_json)['id']

        self.dcos_http_conn_id = dcos_http_conn_id
        self.dcos_robot_user_name = dcos_robot_user_name
        self.dcos_robot_user_pwd = dcos_robot_user_pwd
        self.dcos_iam_api_auth_endpoint = dcos_iam_api_auth_endpoint
        self.dcos_metronome_jobs_api_endpoint = dcos_metronome_jobs_api_endpoint

        # Keeps the currently known state of the deployed Metronome job
        self._job_state = MetronomeJobState.UNDEFINED

    def execute(self, context):
        self.log.info("Executing Metronome operator")
        self.log.info(f"Metronome job ID: {self._metronome_job_id}")
        self.log.info(f"Metronome job definition: {self.metronome_job_json}")

        # Get the authorization token
        auth_token = self._get_auth_token()

        # Get all currently deployed Metronome jobs
        all_metronome_jobs = self._get_all_metronome_jobs(auth_token)

        # Upsert the Metronome job
        if (self._check_job_id_in_list(all_metronome_jobs=all_metronome_jobs)):
            self._update_metronome_job(auth_token=auth_token)
        else:
            self._create_metronome_job(auth_token=auth_token)

        # Start the Metronome job
        job_run_id = self._start_metronome_job(auth_token=auth_token)

        # Polling mechanism. Normally terminates the operator upon job SUCCESS or FAILURE
        while True:
            self._check_and_update_metronome_job_state(auth_token=auth_token, job_run_id=job_run_id)
            if self._job_state == MetronomeJobState.SUCCESS:
                self.log.info(f"Metronome operator finished in state {self._job_state}")
                break
            elif self._job_state == MetronomeJobState.FAILED:
                self.log.exception(f"Metronome operator finished in state {self._job_state}")
                raise ValueError("Metronome job has failed!")
            time.sleep(self.SECONDS_TO_SLEEP_BETWEEN_RETRIES)

    def _get_auth_token(self):
        """
        Returns an authorization token using the provided credentials
        """
        self.log.info("Getting authorization token from DC/OS IAM API")

        http = HttpHook('POST', http_conn_id=self.dcos_http_conn_id)

        self.log.info("Calling HTTP method")
        response = http.run(endpoint=self.dcos_iam_api_auth_endpoint,
                            data=json.dumps({"uid": self.dcos_robot_user_name,
                                             "password": self.dcos_robot_user_pwd}),
                            headers={"Content-Type": "application/json"})
        # Only log the response status code here, not the payload containing the token due to security considerations
        self.log.debug(f"Response is: {str(response.status_code)}")

        auth_token = json.loads(response.text)['token']
        return auth_token

    def _get_all_metronome_jobs(self, auth_token):
        """
        Returns all currently deployed Metronome jobs on the cluster

        :param auth_token: authorization token
        :type auth_token: string
        """
        self.log.info("Getting all Metronome jobs")

        http = HttpHook('GET', http_conn_id=self.dcos_http_conn_id)

        self.log.info("Calling HTTP method")
        response = http.run(endpoint=self.dcos_metronome_jobs_api_endpoint,
                            data=json.dumps({"uid": self.dcos_robot_user_name,
                                             "password": self.dcos_robot_user_pwd}),
                            headers={"Content-Type": "application/json",
                                     "Authorization": f"token={auth_token}"})
        self.log.debug(f"Response is: {str(response)}")
        self.log.info(response.text)

        all_metronome_jobs = json.loads(response.text)
        self.log.debug(f"Metronome jobs are: {str(all_metronome_jobs)}")

        return all_metronome_jobs

    def _check_job_id_in_list(self, all_metronome_jobs):
        """
        Returns whether self._metronome_job_id is present in the given list of all currently deployed Metronome jobs

        :param all_metronome_jobs: list of Metronome jobs
        :type list
        """
        self.log.info("Checking job id in all currently deployed Metronome jobs")

        job_exists = False

        # Iterate through the list of all jobs and check if the job is there
        for job in all_metronome_jobs:
            for attribute, value in job.items():
                if attribute == 'id' and value == self._metronome_job_id:
                    job_exists = True
                    self.log.info("Metronome job found!")

        return job_exists

    def _create_metronome_job(self, auth_token):
        """
        Creates the Metronome job as per definition in self.metronome_job_json

        :param auth_token: authorization token
        :type auth_token: string
        """
        self.log.info("Creating Metronome job")

        http = HttpHook('POST', http_conn_id=self.dcos_http_conn_id)

        self.log.info("Calling HTTP method")
        response = http.run(endpoint=self.dcos_metronome_jobs_api_endpoint,
                            data=self.metronome_job_json,
                            headers={"Content-Type": "application/json",
                                     "Authorization": f"token={auth_token}"})
        self.log.debug(f"Response is: {str(response)}")
        self.log.info(response.text)

    def _update_metronome_job(self, auth_token):
        """
        Updates the existing Metronome job as per definition in self.metronome_job_json

        :param auth_token: authorization token
        :type auth_token: string
        """
        self.log.info("Updating existing Metronome job")

        http = HttpHook('PUT', http_conn_id=self.dcos_http_conn_id)

        self.log.info("Calling HTTP method")
        response = http.run(endpoint=f"{self.dcos_metronome_jobs_api_endpoint}/{self._metronome_job_id}",
                            data=self.metronome_job_json,
                            headers={"Content-Type": "application/json",
                                     "Authorization": f"token={auth_token}"})
        self.log.debug(f"Response is: {str(response)}")
        self.log.info(response.text)

    def _start_metronome_job(self, auth_token):
        """
        Starts the metronome jobs with the ID self.metronome_job_id
        Returns the newly started job run ID

        :param auth_token: authorization token
        :type auth_token: string
        """
        self.log.info("Starting Metronome job")

        http = HttpHook('POST', http_conn_id=self.dcos_http_conn_id)

        self.log.info("Calling HTTP method")
        response = http.run(endpoint=f"{self.dcos_metronome_jobs_api_endpoint}/{self._metronome_job_id}/runs",
                            headers={"Content-Type": "application/json",
                                     "Authorization": f"token={auth_token}"})
        self.log.debug(f"Response is: {str(response)}")
        self.log.info(response.text)

        job_run_id = json.loads(response.text)['id']
        self.log.debug(f"Job run id is: {str(job_run_id)}")
        return job_run_id

    def _check_and_update_metronome_job_state(self, auth_token, job_run_id):
        """
        Checks the status of the Metronome job with the ID job_run_id.
        Updates its state in self._job_state

        :param auth_token: authorization token
        :type auth_token: string
        :param job_run_id: Metronome job run ID
        :type job_run_id: string
        """
        self.log.info("Checking status of Metronome job")

        http = HttpHook('GET', http_conn_id=self.dcos_http_conn_id)

        # Determine whether the job state is in state SUCCESS or FAILED after it finished
        self.log.info("Calling HTTP method")
        response_finished = http.run(endpoint=f"{self.dcos_metronome_jobs_api_endpoint}/{self._metronome_job_id}"
                                              f"?embed=history",
                                     headers={"Content-Type": "application/json",
                                              "Authorization": f"token={auth_token}"})
        self.log.debug(f"Response when job is finished is: {str(response_finished)}")
        self.log.info(response_finished.text)

        successful_finished_runs = json.loads(response_finished.text)['history']['successfulFinishedRuns']
        self.log.debug(f"successfulFinishedRuns are: {str(successful_finished_runs)}")

        failed_finished_runs = json.loads(response_finished.text)['history']['failedFinishedRuns']
        self.log.debug(f"failedFinishedRuns are: {str(failed_finished_runs)}")

        # Check current state
        if self._is_run_id_in_list(successful_finished_runs, job_run_id):
            self._job_state = MetronomeJobState.SUCCESS
        elif self._is_run_id_in_list(failed_finished_runs, job_run_id):
            self._job_state = MetronomeJobState.FAILED
        # Determine whether the job state is in one of the following states after starting:
        # INITIAL, STARTING, ACTIVE
        # NOTE: This endpoint only works when the job is not finished yet, i.e. job state != SUCCESS and != FAILED.
        else:
            self.log.info("Calling HTTP method")
            response_starting = http.run(endpoint=f"{self.dcos_metronome_jobs_api_endpoint}/{self._metronome_job_id}/"
                                                  f"runs/{job_run_id}",
                                         headers={"Content-Type": "application/json",
                                         "Authorization": f"token={auth_token}"})
            self.log.debug(f"Response when job is starting is: {str(response_starting)}")
            self.log.info(response_starting.text)

            # Check current state
            response_starting_json = json.loads(response_starting.text)
            if 'status' in response_starting_json:
                if response_starting_json['status'] == 'INITIAL':
                    self._job_state = MetronomeJobState.INITIAL
                if response_starting_json['status'] == 'STARTING':
                    self._job_state = MetronomeJobState.STARTING
                if response_starting_json['status'] == 'ACTIVE':
                    self._job_state = MetronomeJobState.ACTIVE

        self.log.info(f"Current job state for run ID: {job_run_id} is {self._job_state}")

    def _is_run_id_in_list(self, list_of_runs, job_run_id):
        """
        Returns whether the job_run_id is present in the given list_of_runs

        :param list_of_runs: list of Metronome job runs
        :type list
        :param job_run_id: Metronome job run ID
        :type string
        """
        self.log.info(f"Searching for job run ID {job_run_id} in list of runs")

        run_id_found = False

        for run in list_of_runs:
            for attribute, value in run.items():
                if attribute == 'id' and value == job_run_id:
                    run_id_found = True
                    self.log.info("Job run ID found!")
                    break
        return run_id_found


# Defining the plugin class
class MetronomeOperatorPlugin(AirflowPlugin):
    name = "metronome_operator_plugin"
    operators = [MetronomeOperator]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
