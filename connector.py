# This connector uses the fivetran_connector_sdk module to connect to the US Census API.
# It fetches population projection data from the Census API using the provided API key.
# The connector logs important steps and follows Fivetran's best practices.

from datetime import datetime
import requests as rq
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Define the schema function which configures the schema the connector delivers.
def schema(configuration: dict):
    return [
        {
            "table": "population_projections",
            "primary_key": ["year", "race", "sex", "age"],
            "columns": {
                "year": "INT",             # Year of projection
                "race": "STRING",          # Race
                "sex": "STRING",           # Gender
                "age": "INT",              # Age
                "total_pop": "INT",    # Total population projection
                "last_updated": "UTC_DATETIME"  # Last updated timestamp
            },
        }
    ]

# Define the update function, which is called by Fivetran during each sync.
def update(configuration: dict, state: dict):
    log.info("Census Population Projections Connector: Starting sync")

    # Get the API key from configuration
    api_key = "ac259f7342449a3fcc5b1cfbed52e87e6adfde60"
    
    # Retrieve the cursor from the state to determine the current position in the data sync.
    # If the cursor is not present in the state, start with a default value.
    last_updated_cursor = state.get('last_updated', '2000-01-01T00:00:00Z')
    
    # Current timestamp for updating the records
    current_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # List of parameters to query from the Census API
    # These parameters define different demographic breakdowns
    demo_params = [
        {"YEAR": "1", "ORIGIN": "1", "RACE": "1", "SEX": "1", "AGE": "1"}, # Example: 2017, Hispanic, White, Male, Age 0
        {"YEAR": "1", "ORIGIN": "1", "RACE": "2", "SEX": "1", "AGE": "1"}, # Example: 2017, Hispanic, Black/African American, Male, Age 0
        {"YEAR": "1", "ORIGIN": "1", "RACE": "3", "SEX": "1", "AGE": "1"}, # Example: 2017, Hispanic, AIAN, Male, Age 0
        {"YEAR": "1", "ORIGIN": "1", "RACE": "4", "SEX": "1", "AGE": "1"}, # Example: 2017, Hispanic, Asian, Male, Age 0
        {"YEAR": "1", "ORIGIN": "1", "RACE": "5", "SEX": "1", "AGE": "1"}, # Example: 2017, Hispanic, NHPI, Male, Age 0
        {"YEAR": "1", "ORIGIN": "1", "RACE": "6", "SEX": "1", "AGE": "1"}, # Example: 2017, Hispanic, Two or More Races, Male, Age 0
        {"YEAR": "1", "ORIGIN": "2", "RACE": "1", "SEX": "1", "AGE": "1"}, # Example: 2017, Non-Hispanic, White, Male, Age 0
        {"YEAR": "1", "ORIGIN": "2", "RACE": "2", "SEX": "1", "AGE": "1"}, # Example: 2017, Non-Hispanic, Black/African American, Male, Age 0
    ]
    
    total_records = 0
    
    for params in demo_params:
        try:
            # Construct the API URL with params
            base_url = "https://api.census.gov/data/2017/popproj/pop"
            query_params = f"?get=POP,YEAR,RACE,SEX,AGE&for=us:*&key={api_key}"
            
            # Add demographic filters
           # for key, value in params.items():
           #    query_params += f"&{key}={value}"
            
            full_url = base_url + query_params
            log.fine(f"Requesting URL: {full_url}")
            
            # Make the API request
            response = rq.get(full_url)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            # Parse the JSON response
            data = response.json()
            
            # The first row contains headers, so skip it
            headers = data[0]
            rows = data[1:]
            
            log.info(f"Received {len(rows)} records for params: {params}")
            
            # Process each row of data
            for row in rows:
                # Create a dictionary mapping headers to values
                row_dict = dict(zip(headers, row))
                
                # Extract values and convert to appropriate types
                year = int(row_dict.get("YEAR", 0))
                race = row_dict.get("RACE", "")
                sex = row_dict.get("SEX", "")
                age = int(row_dict.get("AGE", 0))
                pop = int(row_dict.get("POP", 0))
                
                # Map codes to human-readable values for better understanding
                race_map = {"1": "White", "2": "Black", "3": "AIAN", "4": "Asian", "5": "NHPI", "6": "Two or More Races"}
                sex_map = {"1": "Male", "2": "Female"}
                
                race_str = race_map.get(race, race)
                sex_str = sex_map.get(sex, sex)
                
                # Yield an upsert operation to insert/update the row
                yield op.upsert(
                    table="population_projections",
                    data={
                        "year": year,
                        "race": race_str,
                        "sex": sex_str,
                        "age": age,
                        "total_pop": pop,
                        "last_updated": current_time
                    }
                )
                
                total_records += 1
                
        except Exception as e:
            log.warning(f"Error processing data for params {params}: {str(e)}")
    
    log.info(f"Total records processed: {total_records}")
    
    # Save the cursor for the next sync
    yield op.checkpoint(state={
        "last_updated": current_time
    })

# Create the connector object
connector = Connector(update=update, schema=schema)

# Entry point for direct execution (useful for testing)
if __name__ == "__main__":
    connector.debug()
