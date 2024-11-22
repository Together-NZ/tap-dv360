from singer_sdk import Tap, Stream
from singer_sdk.typing import PropertiesList, Property, StringType,DateTimeType
from dv360_121.streams import DV360StandardStream, DV360YoutubeStream



class Tapdv360(Tap):
    """Singer tap for DV360."""
    name = "dv360-121"

    # Only include required parameters like the API base URL
    config_jsonschema = PropertiesList(
        Property("start_date", DateTimeType, required=True, description="Start Date"),
        Property("refresh_token", StringType, description="End Date "),
        Property("advertiser_id", StringType, required=True, description="Advertiser ID"),
        Property("query_standard", StringType, required=False, description="Query Template Path"),
        Property("query_youtube", StringType, required=False, description="Query Template Path"),
    ).to_dict()
    
    def discover_streams(self) -> list[Stream]:
        """Return a list of streams."""
        return [DV360StandardStream(tap=self), DV360YoutubeStream(tap=self)]

if __name__ == "__main__":
    Tapdv360.cli()