# NYC Arrest Data ETL Pipeline

## Overview
This Python script extracts arrest data from the NYC Open Data API, performs data cleaning and transformation, and loads the transformed data into a Snowflake database. The script is designed to run as a standalone process, orchestrating the entire ETL workflow.

## Features
- **Data Extraction**: Fetches data from the NYC Open Data API in batches to handle large datasets efficiently.
- **Data Cleaning**: Cleans the raw arrest data by removing irrelevant columns, dropping rows with missing values, and replacing invalid law category codes.
- **Data Transformation**: Converts all column names to uppercase for Snowflake compatibility.
- **Data Loading**: Loads the cleaned and transformed data into a Snowflake table using the Snowflake Python Connector.
- **Data Visualization**: A Metabase dashboard has been built to visualize and analyze the arrest data stored in Snowflake.

## Prerequisites
- Python 3.x
- Snowflake account
- Access to the NYC Open Data API with an API key

## Installation
1. Clone the repository to your local machine:
    ```
    git clone <repository-url>
    ```
2. Install the required Python packages:
    ```
    pip install -r requirements.txt
    ```
3. Set up environment variables:
    - Create a `.env` file in the project directory.
    - Add your NYC Open Data API key to the `.env` file:
        ```
        NYC_OPEN_DATA_API_KEY=<your-api-key>
        ```
    - Add your Snowflake credentials to the `.env` file:
        ```
        SNOWFLAKE_USER=<snowflake-username>
        SNOWFLAKE_PASSWORD=<snowflake-password>
        SNOWFLAKE_ACCOUNT=<snowflake-account-url>
        SNOWFLAKE_WAREHOUSE=<snowflake-warehouse-name>
        SNOWFLAKE_DATABASE=<snowflake-database-name>
        SNOWFLAKE_SCHEMA=<snowflake-schema-name>
        ```
4. Run the script:
    ```
    python nyc_arrest_data_pipeline.py
    ```

## Usage
- Customize the script according to your specific requirements by modifying the data extraction, cleaning, transformation, and loading functions.
- Adjust the batch size and API URL as needed to optimize data extraction.
- Ensure that the Snowflake table structure matches the cleaned and transformed data before loading.

## License
This project is licensed under the MIT License.