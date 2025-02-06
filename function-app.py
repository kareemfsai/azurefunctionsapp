import logging
import azure.functions as func
from hdbcli import dbapi
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import io
import os

# SAP HANA Connection Details
HANA_HOST = "13b7c15d-848f-40b5-9259-.hna1.prod-eu10.hanacloud.ondemand.com"
HANA_PORT = 443
HANA_USER = "GE148238"
HANA_PASSWORD = "ObOYUBqc1!"
HANA_DATABASE = "GE1438"
QUERY = "SELECT * FROM GE1438.GX_CUSTOMERS "

# Azure Data Lake Details
STORAGE_ACCOUNT_NAME = "fsaicontappdevstr"
STORAGE_ACCOUNT_KEY = "t1KVe3c2+UQ8t4C1+akPfRuYeIIX1IG45d8JJm37BEZhVfxzJ8KLxXryeVtcQHlIRp/EzI84c+Ps+ASto5hs6A=="
FILE_SYSTEM_NAME = "fsaiappstrcontainer1"  # e.g., 'sap-data'
TARGET_FOLDER = "landing-zone/StageHana/GX_CUSTOMERS"

def get_hana_data():
    """Fetch data from SAP HANA and return as a Pandas DataFrame."""
    try:
        connection = dbapi.connect(
            address=HANA_HOST,
            port=HANA_PORT,
            user=HANA_USER,
            password=HANA_PASSWORD
        )
        cursor = connection.cursor()
        cursor.execute(QUERY)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        connection.close()
        return df
    except Exception as e:
        logging.error(f"Error fetching data from SAP HANA: {e}")
        return None

def upload_to_adls(dataframe, file_name):
    """Upload DataFrame to Azure Data Lake Storage as Parquet."""
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=STORAGE_ACCOUNT_KEY
        )
        file_system_client = service_client.get_file_system_client(FILE_SYSTEM_NAME)

        # Create a directory client
        directory_client = file_system_client.get_directory_client(TARGET_FOLDER)
        file_client = directory_client.get_file_client(file_name)

        # Convert DataFrame to Parquet
        buffer = io.BytesIO()
        dataframe.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        # Upload file content
        file_client.upload_data(buffer.getvalue(), overwrite=True)
        file_client.flush_data(len(buffer.getvalue()))
        logging.info(f"File {file_name} uploaded successfully to ADLS.")

    except Exception as e:
        logging.error(f"Error uploading file to ADLS: {e}")

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="hanadataextract")
def hanadataextract(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    df = get_hana_data()
    if df is None:
        return func.HttpResponse("Failed to fetch data from SAP HANA", status_code=500)

    file_name = f"CUSTOMER_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    upload_to_adls(df, file_name)
    return func.HttpResponse(f"Data uploaded to Azure Data Lake as {file_name}", status_code=200)
