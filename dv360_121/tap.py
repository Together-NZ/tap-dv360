from singer_sdk import Tap, Stream
from singer_sdk.typing import PropertiesList, Property, StringType,DateTimeType
from dv360_121.streams import DV360StandardStream, DV360YoutubeStream



class Tapdv360(Tap):
    """Singer tap for DV360."""
    name = "tap-dv360"
    def __init__(self, *, config = None, catalog = None, state = None, parse_env_config = False, validate_config = True, setup_mapper = True):
        
        super().__init__(config=config, catalog=catalog, state=state, parse_env_config=parse_env_config, validate_config=validate_config, setup_mapper=setup_mapper)
        self.shared_data = {}
    # Only include required parameters like the API base URL
    config_jsonschema = PropertiesList(
        Property("start_date", DateTimeType, required=True, description="Start Date"),
        Property("refresh_token", StringType, description="End Date "),
        Property("advertiser_id", StringType, required=True, description="Advertiser ID"),
        Property("query_standard", StringType, required=True, description="Query Template Path"),
        Property("query_youtube", StringType, required=True, description="Query Template Path"),
        Property("service_account",StringType,required=True,description="Service Account"),
    ).to_dict()
    
    def discover_streams(self) -> list[Stream]:
        """Return a list of streams."""
        return [DV360StandardStream(tap=self), DV360YoutubeStream(tap=self)]
if __name__ == "__main__":
    Tapdv360.cli()