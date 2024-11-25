from __future__ import annotations
import json
from typing import Any, Dict, Iterable, Optional
from google.auth import default as google_auth_default
from google.auth.transport.requests import AuthorizedSession, Request
from singer_sdk.streams import RESTStream
import requests
import google
from google.auth.transport.requests import Request
from google.auth import impersonated_credentials
class GoogleADCAuthenticator:
    """Custom authenticator using Google Application Default Credentials with impersonation."""

    def __init__(self, target_service_account):
        # Obtain default ADC credentials
        source_credentials, _ = google.auth.default()
        # Create impersonated credentials
        self.credentials = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=target_service_account,
            target_scopes=["https://www.googleapis.com/auth/doubleclickbidmanager"],
        )
        if not self.credentials.valid:
            self.credentials.refresh(Request())

    def __call__(self, request):
        """Add Authorization header."""
        if not self.credentials.valid:
            self.credentials.refresh(Request())
        request.headers["Authorization"] = f"Bearer {self.credentials.token}"
        return request

class dv360Stream(RESTStream):
    """Stream class for DV360 API."""

    name = "dv360"
    records_jsonpath = "$[*]"  # Adjust based on DV360 API's response structure
    next_page_token_jsonpath = None  # Assuming no pagination for this example

    def __init__(self, tap, name = None, schema = None, path = None):
        super().__init__(tap, name, schema, path)
        self.version = 'v2'
    @property
    def url_base(self) -> str:
        """Base URL for DV360 API."""
        return f"https://doubleclickbidmanager.googleapis.com/{self.version}"

    @property
    def authenticator(self,target_service_account):
        """Provide the custom authenticator."""
        return GoogleADCAuthenticator(target_service_account=target_service_account)

    def get_url(self, context: Optional[Dict[str, Any]]) -> str:
        """Construct the URL for the current operation."""
        operation = context.get("operation")
        query_id = context.get("query_id")
        report_id = context.get("report_id")

        if operation == "query":
            return f"{self.url_base}/queries"
        elif operation == "run":
            return f"{self.url_base}/queries/{query_id}:run"
        elif operation == "poll":
            return f"{self.url_base}/queries/{query_id}/reports/{report_id}"
        else:
            raise ValueError(f"Unknown operation: {operation}")

    def prepare_request_payload(
        self, context: Optional[Dict[str, Any]], next_page_token: Optional[Any] = None
    ) -> Optional[Dict[str, Any]]:
        """Prepare the request payload for POST requests."""
        if context and context.get("operation") == "query":
            return None
            
        return None

    def parse_response(self, response: Any) -> Iterable[Dict[str, Any]]:
        """Parse the response from DV360."""
        if response.status_code != 200:
            raise RuntimeError(f"API call failed with status code {response.status_code}: {response.text}")

        try:
            response_json = response.json()
        except ValueError as e:
            raise RuntimeError(f"Invalid JSON in API response: {response.text}") from e

        # Handle operations (query, poll, retrieve) as needed
        context = self.context or {}
        operation = context.get("operation", "query")
        
        if operation == "query":
            print(f"Parsing query response: {response_json}")  # Debugging
            query_id = response_json.get("queryId")
            if query_id:
                yield {"query_id": query_id}
        elif operation == "poll":
            report_status = response_json.get("metadata", {}).get("status", {}).get("state", "UNKNOWN")
            print(f"Report status: {report_status}")  # Debugging
            if report_status == "DONE":
                yield {"status": "completed"}
            elif report_status == "FAILED":
                raise RuntimeError("Report generation failed.")
            else:
                yield {"status": "in_progress"}
        else:
            yield from super().parse_response(response)




    def request_records(self, context: Optional[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        if context is None:
            context = {}

        # Default operation to 'query' if not set
        context.setdefault("operation", "query")

        # Step 1: Submit Query
        if context["operation"] == "query":
            url = self.get_url(context)
            payload = self.prepare_request_payload(context)
            headers = {"Authorization": f"Bearer {self.authenticator.credentials.token}"}

            response = requests.post(url, json=payload, headers=headers)
            if response.status_code != 200:
                raise RuntimeError(f"API call failed: {response.status_code} - {response.text}")

            query_id = response.json().get("queryId")
            if not query_id:
                raise ValueError("No queryId returned from the API.")

            context["query_id"] = query_id
            context["operation"] = "run"

        # Step 2: Run the Query to Generate a Report
        if context["operation"] == "run":
            query_id = context.get("query_id")
            if not query_id:
                raise ValueError("Query ID is missing for the 'run' operation.")

            url = f"{self.url_base}/queries/{query_id}:run"
            headers = {"Authorization": f"Bearer {self.authenticator.credentials.token}"}

            response = requests.post(url, headers=headers)
            if response.status_code != 200:
                raise RuntimeError(f"Failed to run query: {response.status_code} - {response.text}")

            report_id = response.json().get("key", {}).get("reportId")
            if not report_id:
                raise ValueError("No reportId returned after running the query.")

            context["report_id"] = report_id
            context["operation"] = "poll"

        # Step 3: Poll for Report Status
        if context["operation"] == "poll":
            query_id = context.get("query_id")
            report_id = context.get("report_id")
            if not query_id or not report_id:
                raise ValueError("Query ID or Report ID is missing for the 'poll' operation.")

            url = f"{self.url_base}/queries/{query_id}/reports/{report_id}"
            headers = {"Authorization": f"Bearer {self.authenticator.credentials.token}"}

            while True:
                response = requests.get(url, headers=headers)
                if response.status_code != 200:
                    raise RuntimeError(f"Polling failed: {response.status_code} - {response.text}")

                report_status = response.json().get("metadata", {}).get("status", {}).get("state")
                if report_status == "DONE":
                    context["operation"] = "retrieve"
                    break
                elif report_status == "FAILED":
                    raise RuntimeError("Report generation failed.")
                else:
                    import time
                    time.sleep(10)

        # Step 4: Retrieve Report
        if context["operation"] == "retrieve":
            query_id = context.get("query_id")
            report_id = context.get("report_id")
            if not query_id or not report_id:
                raise ValueError("Query ID or Report ID is missing for the 'retrieve' operation.")

            url = f"{self.url_base}/queries/{query_id}/reports/{report_id}"
            headers = {"Authorization": f"Bearer {self.authenticator.credentials.token}"}

            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise RuntimeError(f"Retrieve failed: {response.status_code} - {response.text}")

            # Process CSV content and yield structured records
            csv_content = response.content.decode("utf-8")
            for row in self._parse_csv_to_records(csv_content):
                yield {
                    "query_id": query_id,
                    "report_id": report_id,
                    **row,  # Add parsed CSV fields here
                }
