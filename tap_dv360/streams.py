"""Stream type classes for tap-dv360."""

from __future__ import annotations
from singer_sdk.typing import PropertiesList, Property, StringType, DateTimeType, NumberType
import typing as t
from importlib import resources
from typing import Iterable, Dict, Optional, Any, List

from singer_sdk import typing as th  # JSON Schema typing helpers
from datetime import datetime, timedelta
from tap_dv360.client import dv360Stream,GoogleADCAuthenticator
import csv
import json
import logging
import requests
from typing import Iterable, Dict, Any
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

    # Add a console handler to see logs in the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
current_date = datetime.now()
NO_SHARED_DATA = False


class DV360StandardStream(dv360Stream):
    def __init__(self, tap, name=None, schema=None, path=None):
        super().__init__(tap, name, schema, path)
        self.shared_data = tap.shared_data
        self.no_data_available = False
    """Define dynamic stream for    DV360 metrics."""
    global NO_SHARED_DATA
    name = "dv360_standard"
    path=f'https://doubleclickbidmanager.googleapis.com/$discovery/rest?version=v2'
    primary_keys: t.ClassVar[list[str]] = ["query_id"]
    replication_key = "Date"
    records_jsonpath = "$[*]"  # Adjust based on DV360 API's response
    next_page_token_jsonpath = None  # Assuming no pagination for this example
    # Example schema definition (flexible for metrics)
    schema = th.PropertiesList(
        th.Property("Date", th.StringType, description="Date of the data in YYYY/MM/DD format"),
        th.Property("Creative", th.StringType, description="Creative name or identifier"),
        th.Property("Creative ID", th.StringType, description="Creative id for creative"),
        th.Property("Advertiser Currency", th.StringType, description="Currency used by the advertiser"),
        th.Property("Insertion Order", th.StringType, description="Name of the insertion order"),
        th.Property("Insertion Order ID", th.StringType, description="Unique ID of the insertion order"),
        th.Property("Insertion Order Status", th.StringType, description="Status of the insertion order (e.g., Active)"),
        th.Property("Line Item", th.StringType, description="Name of the line item"),
        th.Property("Line Item ID", th.StringType, description="Unique ID of the line item"),
        th.Property("Floodlight Activity Name", th.StringType, description="Name of the floodlight activity"),
        th.Property("Floodlight Activity ID", th.StringType, description="Unique ID of the floodlight activity"),
        th.Property("Clicks", th.IntegerType, description="Number of clicks"),
        th.Property("Post-Click Conversions", th.NumberType, description="Number of post-click conversions"),
        th.Property("Impressions", th.IntegerType, description="Number of impressions"),
        th.Property("Post-View Conversions", th.NumberType, description="Number of post-view conversions"),
        th.Property("Total Conversions", th.NumberType, description="Total number of conversions"),
        th.Property("First-Quartile Views (Video)", th.IntegerType, description="Number of video views reaching the first quartile"),
        th.Property("Midpoint Views (Video)", th.IntegerType, description="Number of video views reaching the midpoint"),
        th.Property("Third-Quartile Views (Video)", th.IntegerType, description="Number of video views reaching the third quartile"),
        th.Property("Complete Views (Video)", th.IntegerType, description="Number of completed video views"),
        th.Property("Revenue (Adv Currency)", th.NumberType, description="Revenue in advertiser currency"),
        th.Property("CM360 Post-Click Revenue", th.NumberType, description="Revenue from post-click events in CM360"),
        th.Property("CM360 Post-View Revenue", th.NumberType, description="Revenue from post-view events in CM360"),
        th.Property("Video Plays (Video)", th.IntegerType, description="Number of video plays"),
    ).to_dict()
    @property
    def authenticator(self):
        self.service_account = self.config.get("service_account")
        return GoogleADCAuthenticator(target_service_account=self.service_account)
    def prepare_request_payload(
        self,
        context: Optional[Dict[str, Any]],
        next_page_token: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Prepare the request payload for DV360 queries."""
        # Fetch configuration values
        start_date_str = self.config.get("start_date")  # Assumes "start_date" is in ISO format
        end_date_str = self.config.get("end_date")
        advertiser_id = self.config.get("advertiser_id")
        query_path = self.config.get("query_standard")

        # Parse start and end dates
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = (
            datetime.strptime(end_date_str, "%Y-%m-%d")
            if end_date_str
            else datetime.now()
        )
        
        # Load and populate query template
        try:
            with open(query_path, "r") as template_file:
                query_template = json.load(template_file)
            # populate the filter with advertiser id on standard stream
            filters=[]
            if advertiser_id !=None:
                filters.extend([
                    {"type":"FILTER_ADVERTISER","value":advertiser_id}
                    
                ]) 
             
            query_template["params"]["filters"]=filters 
            # Populate custom start and end dates
            query_template["metadata"]["dataRange"]["customStartDate"]["year"] = start_date.year
            query_template["metadata"]["dataRange"]["customStartDate"]["month"] = start_date.month
            query_template["metadata"]["dataRange"]["customStartDate"]["day"] = start_date.day

            query_template["metadata"]["dataRange"]["customEndDate"]["year"] = end_date.year
            query_template["metadata"]["dataRange"]["customEndDate"]["month"] = end_date.month
            query_template["metadata"]["dataRange"]["customEndDate"]["day"] = end_date.day


            logger.debug(f"Prepared query payload: {json.dumps(query_template, indent=2)}")

        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load or parse the query template: {e}")
            raise RuntimeError("Query template is missing or malformed.") from e

        # Return the prepared payload only for 'query' operation
        if context and context.get("operation") == "query":
            return query_template

        return None




    def _parse_csv_to_records(self, csv_content: str) -> Iterable[Dict[str, Any]]:
        """Convert CSV content into a list of records, dynamically extracting metric names."""
        logger.info("Starting to parse CSV content.")
        global NO_SHARED_DATA
        # If the content looks like JSON, extract the `googleCloudStoragePath`
        if csv_content.strip().startswith("{"):
            logger.info("Response appears to be JSON; attempting to extract CSV URL.")
            try:
                response_json = json.loads(csv_content)
                csv_url = response_json.get("metadata", {}).get("googleCloudStoragePath")
                if not csv_url:
                    logger.error("CSV URL not found in response.")
                    raise ValueError("CSV URL not found in response.")

                logger.info(f"Downloading CSV from URL: {csv_url}")
                response = requests.get(csv_url)
                if response.status_code != 200:
                    logger.error(f"Failed to download CSV: {response.status_code} - {response.text}")
                    raise RuntimeError(f"Failed to download CSV: {response.status_code} - {response.text}")

                csv_content = response.text  # Replace with downloaded CSV content
                logger.info("CSV content downloaded successfully.")

            except json.JSONDecodeError as e:
                logger.exception("Failed to decode JSON response.")
                raise RuntimeError("Invalid JSON in API response.") from e

        # Parse the CSV content
        try:
            filters=set()
            reader = csv.DictReader(csv_content.splitlines())
            logger.info("Parsing CSV content into rows.")
            # Reinitialize reader to start from the beginning
            reader = csv.DictReader(csv_content.splitlines())            
            self.shared_data["filters"] = filters
            self._tap.shared_data["filters"] = filters
            # Process all rows and extract metric data
            for row in reader:
                
                if not row.get("Date"):
                    break
                else:
                    date_value = row.get("Date")
                    if date_value == "No data returned by the reporting service.":
                        logger.warning("No data available from the report. Returning empty iterator.")
                        NO_SHARED_DATA = True
                        return  # Stop processing, no valid data
                    if row.get("Creative")=="Unknown" and row.get("Insertion Order") and row.get("Insertion Order ID") and row.get("Date"):
                        # Check if the row matches the condition to populate filters
                        filters.add((row.get("Insertion Order"), row.get("Insertion Order ID")))
                        logger.info(f"Added to filters: (Insertion Order: {row.get('Insertion Order')}, "
                        f"Insertion Order ID: {row.get('Insertion Order ID')})")
                        logger.debug(f"Current state of filters: {filters}")
                    yield row
            
        except Exception as e:
            logger.exception("Error occurred while parsing CSV content.")
            raise e


class DV360YoutubeStream(dv360Stream):
    """Define dynamic stream for DV360 YouTube metrics."""

    def __init__(self, tap, name=None, schema=None, path=None):
        super().__init__(tap, name, schema, path)
        self.shared_data = tap.shared_data
        logger.info(f"Shared data: {self.shared_data}")

    name = "dv360_youtube"
    path = "https://doubleclickbidmanager.googleapis.com/$discovery/rest?version=v2"
    primary_keys: t.ClassVar[list[str]] = ["query_id"]
    replication_key = "Date"
    records_jsonpath = "$[*]"  # Adjust based on DV360 API's response
    next_page_token_jsonpath = None  # Assuming no pagination for this example

    # Example schema definition (flexible for metrics)
    schema = th.PropertiesList(
        th.Property("Advertiser", th.StringType, description="Name of the advertiser"),
        th.Property("Date", th.StringType, description="Date of the data in YYYY/MM/DD format"),
        th.Property("YouTube Ad ID", th.StringType, description="ID of the YouTube ad"),
        th.Property("YouTube Ad", th.StringType, description="Name of the YouTube ad"),
        th.Property("Advertiser Currency", th.StringType, description="Currency used by the advertiser"),
        th.Property("Insertion Order", th.StringType, description="Name of the insertion order"),
        th.Property("Insertion Order ID", th.StringType, description="ID of the insertion order"),
        th.Property("Insertion Order Status", th.StringType, description="Status of the insertion order (e.g., Active, Paused)"),
        th.Property("YouTube Ad Group", th.StringType, description="Name of the YouTube ad group"),
        th.Property("YouTube Ad Group ID", th.StringType, description="ID of the YouTube ad group"),
        th.Property("Line Item", th.StringType, description="Name of the line item"),
        th.Property("Line Item ID", th.StringType, description="ID of the line item"),
        th.Property("Clicks", th.StringType, description="Number of clicks on the ads"),
        th.Property("Impressions", th.StringType, description="Number of impressions for the ads"),
        th.Property("First-Quartile Views (Video)", th.StringType, description="Number of video views that reached the first quartile"),
        th.Property("Midpoint Views (Video)", th.StringType, description="Number of video views that reached the midpoint"),
        th.Property("Third-Quartile Views (Video)", th.StringType, description="Number of video views that reached the third quartile"),
        th.Property("Complete Views (Video)", th.StringType, description="Number of video completions"),
        th.Property("Revenue (Adv Currency)", th.StringType, description="Revenue generated in the advertiser's currency"),
        th.Property("TrueView", th.StringType, description="True view"),
    ).to_dict()

    @property
    def authenticator(self):
        self.service_account = self.config.get("service_account")
        return GoogleADCAuthenticator(target_service_account=self.service_account)

    def prepare_request_payload(
        self,
        context: Optional[Dict[str, Any]],
        next_page_token: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Prepare the request payload for DV360 queries."""
        # Fetch configuration values
        start_date_str = self.config.get("start_date")  # Assumes "start_date" is in ISO format
        end_date_str = self.config.get("end_date")
        logger.info(f"start_date: {start_date_str}, end_date: {end_date_str}")

        # Get shared filters from DV360StandardStream
        filters = self._tap.shared_data.get("filters", [])



        query_path = self.config.get("query_youtube")

        # Parse start and end dates
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else datetime.now()

        # Load and populate query template
        try:
            with open(query_path, "r") as template_file:
                query_template = json.load(template_file)

            # Populate filters
            formatted_filters = [
                {"type": "FILTER_INSERTION_ORDER", "value": io_id}
                for io, io_id in filters
            ]
            query_template["params"]["filters"] = formatted_filters

            # Populate custom start and end dates
            query_template["metadata"]["dataRange"]["customStartDate"]["year"] = start_date.year
            query_template["metadata"]["dataRange"]["customStartDate"]["month"] = start_date.month
            query_template["metadata"]["dataRange"]["customStartDate"]["day"] = start_date.day

            query_template["metadata"]["dataRange"]["customEndDate"]["year"] = end_date.year
            query_template["metadata"]["dataRange"]["customEndDate"]["month"] = end_date.month
            query_template["metadata"]["dataRange"]["customEndDate"]["day"] = end_date.day

            logger.debug(f"Prepared query payload: {json.dumps(query_template, indent=2)}")

        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load or parse the query template: {e}")
            raise RuntimeError("Query template is missing or malformed.") from e

        # Return the prepared payload only for 'query' operation
        if context and context.get("operation") == "query":
            return query_template

        return None

    def _parse_csv_to_records(self, csv_content: str) -> Iterable[Dict[str, Any]]:
        """Convert CSV content into a list of records, dynamically extracting metric names."""
        logger.info("Starting to parse CSV content.")

        # If response is JSON, extract CSV URL
        if csv_content.strip().startswith("{"):
            logger.info("Response appears to be JSON; attempting to extract CSV URL.")
            try:
                response_json = json.loads(csv_content)
                csv_url = response_json.get("metadata", {}).get("googleCloudStoragePath")
                if not csv_url:
                    raise ValueError("CSV URL not found in response.")

                logger.info(f"Downloading CSV from URL: {csv_url}")
                response = requests.get(csv_url)
                if response.status_code != 200:
                    raise RuntimeError(f"Failed to download CSV: {response.status_code} - {response.text}")

                csv_content = response.text  # Replace with downloaded CSV content
                logger.info("CSV content downloaded successfully.")

            except json.JSONDecodeError as e:
                raise RuntimeError("Invalid JSON in API response.") from e

        # Parse the CSV content
        try:
            reader = csv.DictReader(csv_content.splitlines())
            logger.info("Parsing CSV content into rows.")
            logger.info(f"NO_SHARED_DATA: {NO_SHARED_DATA}")

            for row in reader:
                if NO_SHARED_DATA:
                    logger.info("NO_SHARED_DATA is True. Returning empty iterator.")
                    return
                if not row.get("Date"):
                    break
                else:
                    logger.debug(f"Processing row: {row}")
                    yield row

        except Exception as e:
            logger.exception("Error occurred while parsing CSV content.")
            raise e
