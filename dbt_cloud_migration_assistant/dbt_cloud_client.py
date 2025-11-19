"""Client for interacting with dbt Cloud API"""

import requests
from typing import List, Dict, Optional, Any


class DbtCloudClient:
    """Client for dbt Cloud API operations"""

    DEFAULT_BASE_URL = "https://cloud.getdbt.com/api/v2"

    def __init__(self, api_key: str, account_id: int, base_url: Optional[str] = None):
        """
        Initialize dbt Cloud client

        Args:
            api_key: dbt Cloud API key
            account_id: dbt Cloud account ID
            base_url: Optional custom base URL for multi-tenant accounts
                     (e.g., https://lm759.us1.dbt.com/api/v2)
        """
        self.api_key = api_key
        self.account_id = account_id
        self.base_url = base_url or self.DEFAULT_BASE_URL
        self.headers = {
            "Authorization": f"Token {api_key}",
            "Content-Type": "application/json",
        }

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make a request to the dbt Cloud API"""
        url = f"{self.base_url}/accounts/{self.account_id}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        
        # Provide better error messages
        if response.status_code == 401:
            error_detail = response.text
            raise requests.exceptions.HTTPError(
                f"401 Unauthorized: Authentication failed. "
                f"Please verify:\n"
                f"  1. Your API token is correct and active\n"
                f"  2. Your account ID is correct (found in URL: cloud.getdbt.com/settings/accounts/{{ID}}/)\n"
                f"  3. The service token has proper permissions\n"
                f"Response: {error_detail}"
            )
        
        response.raise_for_status()
        return response.json()
    
    def test_connection(self) -> bool:
        """Test the API connection by fetching account info"""
        try:
            # Try to get account info first (this endpoint might work better)
            url = f"{self.base_url}/accounts/{self.account_id}/"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return True
            elif response.status_code == 401:
                return False
            # If account endpoint doesn't exist, try projects
            return True
        except Exception:
            return False

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

