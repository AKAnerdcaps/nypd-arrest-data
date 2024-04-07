import os
import requests
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def fetch_arrest_data(api_url, headers, limit):
    """Fetches data from the NYC Open Data API in batches.

    Args:
        api_url (str): URL of the NYC Open Data API endpoint.
        headers (dict): Dictionary containing API request headers.
        limit (int): Maximum number of records to fetch per request.

    Returns:
        pandas.DataFrame: A pandas DataFrame containing the arrest data.
    """

    offset = 0
    all_records = []
    while True:
        params = {"$limit": limit, "$offset": offset}
        response = requests.get(api_url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            all_records.extend(data)

            print(f"Fetched {len(data)} records.")

            if len(data) < limit:
                break
            offset += limit

        else:
            print(f"API request failed with status code: {response.status_code}")
            break
    return pd.DataFrame(all_records)


def clean_and_transform_data(arrest_data):
    """Cleans and transforms the arrest data.

    Args:
        arrest_data (pandas.DataFrame): DataFrame containing raw arrest data.

    Returns:
        pandas.DataFrame: Cleaned and transformed DataFrame.
    """

    # Drop irrelevant columns with descriptive names
    irrelevant_columns = [
        "geocoded_column",
        ":@computed_region_f5dn_yrer",
        ":@computed_region_yeji_bk3q",
        ":@computed_region_92fq_4b7q",
        ":@computed_region_sbqj_enih",
        ":@computed_region_efsh_h5xi",
    ]
    arrest_data = arrest_data.drop(columns=irrelevant_columns)

    # Drop rows with missing pd_cd or ky_cd
    arrest_data = arrest_data.dropna(subset=["pd_cd", "ky_cd"])

    # Replace invalid law category codes with a clear description
    arrest_data["law_cat_cd"] = arrest_data["law_cat_cd"].replace(
        ["9", "I"], "Unknown")

    # Convert all column names to uppercase for Snowflake compatibility
    arrest_data = arrest_data.rename(columns=str.upper)

    return arrest_data


def load_data_to_snowflake(snowflake_context, arrest_data, table_name, database_name, schema_name):
    """Loads the arrest data into a Snowflake table.

    Args:
        snowflake_context (snowflake.connector.SnowflakeConnection):
            Snowflake connection context.
        arrest_data (pandas.DataFrame): DataFrame containing the arrest data.
        table_name (str): Name of the Snowflake table to load the data into.
        database_name (str): Name of the Snowflake Database to load the data into. (Optional)
        schema_name (str): Name of the Snowflake Schema to load the data into. (Optional)
    """

    try:
        write_pandas(snowflake_context, arrest_data, table_name, database_name, schema_name)
    except Exception as e:
        print(f"Error writing data to Snowflake: {e}")


def main():
    """Main function to orchestrate data extraction, cleaning, and loading."""

    load_dotenv()

    api_key = os.getenv("NYC_OPEN_DATA_API_KEY")
    api_url = "https://data.cityofnewyork.us/resource/uip8-fykc.json"
    headers = {"X-App-Token": api_key}
    batch_size = 50000

    try:
        raw_arrest_data = fetch_arrest_data(api_url, headers, batch_size)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during API request: {e}")
        return

    # Clean and transform the data
    cleaned_arrest_data = clean_and_transform_data(raw_arrest_data)

    # Connect to Snowflake
    SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

    try:
        ctx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        snowflake_connected = True  # Set flag if connection successful

        # Load the cleaned data into the Snowflake table
        load_data_to_snowflake(ctx, cleaned_arrest_data, "NYPD_ARRESTS", SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)

    except Exception as e:
        print(f"Error connecting to Snowflake or writing data: {e}")

    finally:
        if snowflake_connected:
            ctx.close()


if __name__ == "__main__":
    main()
