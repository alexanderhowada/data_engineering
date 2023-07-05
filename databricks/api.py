import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from copy import deepcopy


def _default_http_adapter(http_adapter: HTTPAdapter = None):
    """Creates a default HTTPAdapter with Retries."""

    if http_adapter is None:
        http_adapter = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=5
        )

    adapter = HTTPAdapter(max_retries=http_adapter)
    session = requests.Session()
    session.mount("https://", adapter)

    return adapter, session


class DatabricksBaseAPI:
    """Base wrapper class for the Databricks API."""

    endpoints = {
        "jobs_list": {"operation": "get", "endpoint": "/api/2.1/jobs/list"}
    }

    def __init__(self, host: str, token: str, http_adapter=None):
        """
        Args:
            host: the Databricks http host: (https://3308...gcs.databricks.com)
            token: the Databricks personal access token.
            http_adapter: if None, defaults to 5 retries.
        """

        self.host = host.rstrip('/')
        self.token = token
        self.adapter, self.session = _default_http_adapter(http_adapter)

        # self._build_endpoint_methods()

    # Not working as intended
    # def _build_endpoint_methods(self):
    #     """Build the endpoints keys as methods."""

    #     for k in self.endpoints.keys():
    #         setattr(
    #             self, k,
    #             deepcopy(lambda endpoint=k, **kwargs: self.__call__(endpoint=k, **kwargs))
    #         )
    #         setattr(
    #             self, f"{k}_json",
    #             deepcopy(lambda endpoint=k, **kwargs: self.__getitem__(endpoint=k, **kwargs))
    #         )

    def get_headers(self, h={}):
        """Adds Authorization to the header."""

        if "Authorization" not in h.keys():
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def request(self, operation: str, url: str, **kwargs) -> requests.Response:
        """Request data to the Databricks endpoint.

        Requests data to the Databricks url.
        Supports operations such as "get", "post", "update", etc.
        See self.endpoints for the operations.
        
        Args:
            operation: the operation of the endpoint (ex: "get" or "post").
            url: the url to request (ex: "https://33...gcs.databricks.com/api/2.1/jobs/list).
            kwargs: keyword arguments for requests.operation.
        
        Returns:
            The response of the request as in resquests.operation.
        """

        if "headers" not in kwargs.keys():
            kwargs["headers"] = {}
        kwargs["headers"] = self.get_headers(kwargs["headers"])

        operation = getattr(self.session, operation)
        self.last_response = operation(url, **kwargs)

        if self.last_response.status_code >= 400:
            m = f"Status code {self.last_response.status_code}\n"
            m += self.last_response.text
            raise Exception(m)

        return self.last_response
    
    def __call__(self, endpoint: str, **kwargs) -> requests.Response:
        """Call the implemented endpoint.

        Calls the implemented endpoint by its key name.
        See self.endpoints.keys() for the possible endpoints.

        Args:
            endpoint: any value in self.endpoints.keys().
            kwargs: keyword arguments for requests.operation.

        Returns:
            The request response as in requests.operation.
        """

        operation = self.endpoints[endpoint]["operation"]
        url = self.host + self.endpoints[endpoint]["endpoint"]
        return self.request(operation, url, **kwargs)
    
    def __getitem__(self, *args, **kwargs) -> dict:
        """Same as __call__, but returns requests.Response.json().

        Refer to __call__ for more information.

        Args:
            Same arguments as in self.__call__.

        Returns:
            Dictionary as in requests.Response.json().
        """

        return self(*args, **kwargs).json()

class DatabricksJobsAPI(DatabricksBaseAPI):
    """Wraps the Databricks jobs API.

    Example:

    d_api = DatabricksJobsAPI(
        dbutils.secrets.get(scope="pat", key="host"),
        dbutils.secrets.get(scope="pat", key="wmc_eng_team_default")
    )

    j = d_api.get_job_name("Clone of loreal_workflow")
    print(j)
    """

    endpoints = {
        "list": {"operation": "get", "endpoint": "/api/2.1/jobs/list"},
        "get": {"operation": "get", "endpoint": "/api/2.1/jobs/get"},
        "create": {"operation": "post", "endpoint": "/api/2.1/jobs/create"},
        "update": {"operation": "post", "endpoint": "/api/2.1/jobs/update"},
        "reset": {"operation": "post", "endpoint": "/api/2.1/jobs/reset"},
        "delete": {"operation": "post", "endpoint": "/api/2.1/jobs/delete"},
    }

    def __call__(self, endpoint, *args, **kwargs) -> requests.Response:
        """Call the implemented endpoint.

        Calls the implemented endpoint by its key name.
        See self.endpoints.keys() for the possible endpoints.
        The "create" endpoint must be called by its owm method.

        Args:
            args: arguments for superclass's __call__.
            kwargs: keyword arguments for superclass's __call__

        Returns:
            The request response as in requests.operation.
        """

        if endpoint == "create" and self.job_name_exists(kwargs["json"]):
            raise Exception("Job already exists.")

        return super().__call__(endpoint=endpoint, *args, **kwargs)
    
    def job_name_exists(self, j):

        jobs_list = self.get_jobs_list(j={"name": j["name"], "limit": 25})

        if len(jobs_list) >= 1:
            return True
        
        return False

    def get_jobs_list(self, j={"limit": 25}) -> list[dict]:
        """Get the list of the jobs by running through all the pages.

        Args:
            j: the dictionary with the parameters for the endpoint request.

        Returns:
            List of the jobs (workflows).
        """

        r = self("list", json=j)
        
        if "jobs" not in r.json().keys():
            return []
        
        json_list = r.json()["jobs"]

        while r.json()["has_more"]:
            j["page_token"] = r.json()['next_page_token']
            r = self("list", json=j)
            json_list += r.json()["jobs"]

        return json_list
    
    def get_job(self, job_id: int):
        """Get a job (workflow) by its id."""

        j = {"job_id": job_id}
        return self("get", json=j).json()
    
    def get_job_id(self, name: str) -> int:
        """Get job id by its name.

        Args:
            name: the name of the job.
        
        Returns:
            The job id.

        Raises:
            Exception: if the number of jobs is different then 1.
        """

        jl = self.get_jobs_list(j={"name": name})

        if len(jl) != 1:
            job_list = {
                j["job_id"]: j["settings"]["name"] for j in jl
            }
            raise Exception(f"Job does not exists or more than 1 job.\n {job_list}")

        return jl[0]["job_id"]
    
    def get_job_name(self, name: str) -> dict:
        """Get job (workflow) by its name.

        Returns a single job by its name.
        Notice that Databricks allows many jobs with the same name,
        however this library do not, since it does not make sense for our business.

        Args:
            name: the name of the job.
        
        Returns:
            Dicionary with the job configuration.
        """

        job_id = self.get_job_id(name)
        job = self.get_job(job_id)

        return job
    
class DatabricksGitAPI(DatabricksBaseAPI):
    """Wraps the Databricks Git API.

    Example:

    git_api = DatabricksGitAPI(
        dbutils.secrets.get(scope="pat", key="host"),
        dbutils.secrets.get(scope="pat", key="key")
    )
    print(git_api.get())
    print(git_api.update("TokenHereTokenHere", "someone@gmail.com", "GitHub"))
    """

    endpoints = {
        "get": {"operation": "get", "endpoint": "/api/2.0/git-credentials"},
        "create": {"operation": "post", "endpoint": "/api/2.0/git-credentials"},
        "update": {"operation": "patch", "endpoint": "/api/2.0/git-credentials/{credential_id}"},
    }

    def get(self):
        return self("get").json()

    def create(self, personal_access_token: str, git_username: str, git_provider: str):
        """Create a credential

        Args:
            personal_access_token: The GitHub token.
            git_username: git user name.
            git_provider: ex: GitHub.

        Returns:
            Dict with the JSON of the response.
        """

        j = {
            "personal_access_token": personal_access_token,
            "git_username": git_username,
            "git_provider": git_provider
        }
        return self("create", json=j).json()
    
    def update(self, personal_access_token: str, git_username: str, git_provider: str):
        """Create a credential

        Args:
            personal_access_token: The GitHub token.
            git_username: git user name.
            git_provider: ex: GitHub.

        Returns:
            Dict with the JSON of the response.
        """

        git_config = self.get()["credentials"][0]
        credential_id = git_config["credential_id"]
        
        operation = self.endpoints["update"]["operation"]
        url = self.host + self.endpoints["update"]["endpoint"].format(credential_id=credential_id)
        j = {
            "personal_access_token": personal_access_token,
            "git_username": git_username,
            "git_provider": git_provider
        }

        return self.request(operation, url, json=j)

