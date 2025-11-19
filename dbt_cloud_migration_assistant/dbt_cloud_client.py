"""Client for interacting with dbt Cloud API"""

import requests
from typing import List, Dict, Optional, Any


class DbtCloudClient:
    """Client for dbt Cloud API operations"""

    BASE_URL = "https://cloud.getdbt.com/api/v2"

    def __init__(self, api_key: str, account_id: int):
        """
        Initialize dbt Cloud client

        Args:
            api_key: dbt Cloud API key
            account_id: dbt Cloud account ID
        """
        self.api_key = api_key
        self.account_id = account_id
        self.headers = {
            "Authorization": f"Token {api_key}",
            "Content-Type": "application/json",
        }

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make a request to the dbt Cloud API"""
        url = f"{self.BASE_URL}/accounts/{self.account_id}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_projects(self) -> List[Dict[str, Any]]:
        """Fetch all projects from dbt Cloud"""
        data = self._make_request("projects/")
        return data.get("data", [])

    def get_project(self, project_id: int) -> Dict[str, Any]:
        """Fetch a specific project by ID"""
        data = self._make_request(f"projects/{project_id}/")
        return data.get("data", {})

    def get_jobs(self, project_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch all jobs, optionally filtered by project"""
        params = {}
        if project_id:
            params["project_id"] = project_id
        data = self._make_request("jobs/", params=params)
        return data.get("data", [])

    def get_job(self, job_id: int) -> Dict[str, Any]:
        """Fetch a specific job by ID"""
        data = self._make_request(f"jobs/{job_id}/")
        return data.get("data", {})

    def get_environments(self, project_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch all environments, optionally filtered by project"""
        params = {}
        if project_id:
            params["project_id"] = project_id
        data = self._make_request("environments/", params=params)
        return data.get("data", [])

    def get_environment(self, environment_id: int) -> Dict[str, Any]:
        """Fetch a specific environment by ID"""
        data = self._make_request(f"environments/{environment_id}/")
        return data.get("data", {})

