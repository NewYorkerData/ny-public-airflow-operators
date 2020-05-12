import hdfs
from airflow import configuration
from airflow import settings
from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.log.logging_mixin import LoggingMixin


class WebHDFSClient(LoggingMixin):
    """
    A  web hdfs client which uses WebHDFS REST API to interact with HDFS cluster.

    The library is using `this api
    <https://hdfscli.readthedocs.io/en/latest/api.html>`__.


    :param hosts: List of namenodes separated by ';'
    :type hosts: str
    :param port: Connection port
    :type port: int
    :param user: Login name
    :type user: str
    :param client_type: Type of client.
    :type client_type: str
    """

    def __init__(self, hosts=None, port=None, user=None, client_type=None):
        super().__init__()
        self.hosts = hosts
        self.port = port
        self.user = user
        self.client_type = client_type
        self._client = None

    @property
    def url(self):
        # the hdfs package allows it to specify multiple namenodes by passing a string containing
        # multiple namenodes separated by ';'
        hosts = self.hosts.split(";")
        urls = [f"http://{host}:{str(self.port)}" for host in hosts]
        return ";".join(urls)

    @property
    def client(self):
        self.log.info("Getting WebHDFS client.")
        if self._client is None:
            self._client = self.create()
            self.log.debug("WebHDFS client has been created.")

        return self._client

    def create(self):
        """
        Creates webhdfs client instance.
        Concrete implementation depends on a client_type parameter,
        if it's kerberos, then KerberosClient is created, otherwise InsecureClient.

        :return hdfs client:
        """
        if self.client_type == 'kerberos':
            from hdfs.ext.kerberos import KerberosClient
            return KerberosClient(url=self.url)
        else:
            return hdfs.InsecureClient(url=self.url, user=self.user)

    def exists(self, path):
        """
        Returns true if the path exists and false otherwise.
        """
        try:
            self.log.info(f"Checking path {path} status")
            self.client.status(path)
            self.log.info(f"Path {path} exists")
            return True
        except hdfs.util.HdfsError as e:
            if str(e).startswith("File does not exist: "):
                self.log.info(f"Path {path} doesn't exist")
                return False
            else:
                raise e


class HDFSHook(BaseHook):
    """
    Implementation of airflow hook, which uses hdfscli hdfs client to interact with HDFS cluster.

    :param hdfs_conn_id: Connection id to fetch connection info
    :type hdfs_conn_id: str
    """

    def __init__(self, hdfs_conn_id=None):
        super(HDFSHook, self).__init__(None)

        self.hdfs_conn_id = hdfs_conn_id
        self._conn = None

    def get_conn(self):
        """
        Returns connection to HDFS cluster.
        """
        self.log.info("Getting connection to hdfs.")
        if self._conn is None:
            self._conn = self.create_connection()

        return self._conn

    def create_connection(self):
        """
        Creates client to connect to HDFS cluster using WebHDFS REST API.
        """
        self.log.info("Creating client for connection to hdfs.")
        hdfs_params = {}
        security_conf = configuration.conf.get("core", "security")
        # Configure kerberos security if used by Airflow.
        if security_conf == "kerberos":
            hdfs_params["hadoop.security.authentication"] = "kerberos"

        conn_params = self.get_connection(self.hdfs_conn_id)
        conn_extra_params = conn_params.extra_dejson

        # Extract hadoop parameters from extra.
        hdfs_params.update(conn_extra_params.get("pars", {}))

        # Extract high-availability config if given.
        ha_params = conn_extra_params.get("ha", {})
        hdfs_params.update(ha_params.get("conf", {}))

        # Creates client to connect to hdfs.
        conn = WebHDFSClient(
            hosts=ha_params.get("host") or conn_params.host or None,
            port=conn_params.port or None,
            user=conn_params.login or None,
            client_type=security_conf
        )
        self.log.debug("Successfully created hdfs client")
        return conn


class NYHDFSSensor(BaseSensorOperator):
    """
    Extension of existing HdfsSensor with replacement of HDFSHook with new one,
    which uses python3 compatible hdfs client - hdfscli.

    :param filepaths: list of file paths, which need to be detected.
    :type filepaths: list[str]
    :param hdfs_conn_id: The connection to run the sensor against
    :type hdfs_conn_id: str
    :param hook: Interface to interact with HDFS
    :type hook: :class: `HDFSHook`
    """

    template_fields = ('filepaths',)
    ui_color = settings.WEB_COLORS['LIGHTBLUE']

    @apply_defaults
    def __init__(self,
                 filepaths,
                 hdfs_conn_id='hdfs_default',
                 hook=HDFSHook,
                 *args,
                 **kwargs):
        super(NYHDFSSensor, self).__init__(*args, **kwargs)
        self.filepaths = filepaths
        self.hdfs_conn_id = hdfs_conn_id
        self.hook = hook

    def poke(self, context):
        """ check paths for existence, returns True only if all paths exist """
        exist = False
        try:
            conn = self.hook(self.hdfs_conn_id).get_conn()
            self.log.info(f"Poking for files {self.filepaths}.")
            exist = all(map(conn.exists, self.filepaths))
            self.log.debug(f"Files {self.filepaths} exist={exist}")
        except:
            self.log.exception(f"Caught an exception while trying to find the files {self.filepaths}!")
        return exist


class HDFSSensorPlugin(AirflowPlugin):
    """ Plugin to integrate hdfs sensor to airflow """
    name = "hdfs_sensor_plugin"
    sensors = [NYHDFSSensor]
    hooks = [HDFSHook]
