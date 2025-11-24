## OCM EV Data Extractor - Cloud Function
import functions_framework
import os
import time
import requests
import pandas as pd
import numpy as np
from google.cloud import storage, bigquery
from datetime import datetime
from google.api_core.exceptions import NotFound

## Configurations
bucket_name = "ev-tracker-data-london-ocm-only"
project_name = "fiery-atlas-472112-s9"  ## Update this Accordingly
dataset_name = "ev_data_ocm_only"
table_name = "ev_chargers_ocm_only_cleaned"

## Environment Variables
OCM_API_KEY = os.getenv("OCM_API_KEY")

## API bounding box for Central London
MIN_LAT, MAX_LAT = 51.48, 51.55
MIN_LNG, MAX_LNG = -0.20, -0.02
STEP = 0.01
OCM_API_URL = "https://api.openchargemap.io/v3/poi/"

## Safe GET with exponential backoff (handles 429 errors)
def safe_get(url, params, max_retries=5):
    retry = 0
    while retry < max_retries:
        try:
            response = requests.get(url, params=params)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_time = int(retry_after) if retry_after else 2 ** retry
                print(f"429 Too Many Requests. Retrying in {wait_time} sec...")
                time.sleep(wait_time)
                retry += 1
                continue

            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            wait_time = 2 ** retry
            print(f"Request failed ({e}). Retry in {wait_time} sec...")
            time.sleep(wait_time)
            retry += 1

    print("Max retries exceeded. Skipping this request.")
    return None


## Upload CSV to Cloud Storage
def upload_to_gcs(df: pd.DataFrame, filename: str):
    client = storage.Client(project=project_name)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
    return f"gs://{bucket_name}/{filename}"


## Load DataFrame to BigQuery
def load_to_bigquery_incremental(df: pd.DataFrame):
    client = bigquery.Client(project=project_name)
    table_id = f"{project_name}.{dataset_name}.{table_name}"

    ## Check if table exists
    try:
        table = client.get_table(table_id) 
        table_exists = True
    except NotFound:
        print(f"Table {table_id} not found. It will be created.")
        table_exists = False

    if table_exists:
        ## Get existing Place_IDs
        query = f"SELECT Place_ID FROM `{table_id}`"
        existing_ids = [row.Place_ID for row in client.query(query).result()]
        df_new = df[~df["Place_ID"].isin(existing_ids)]
        if df_new.empty:
            print("No new records to append.")
            return table_id
    else:
        ## If table doesn't exist, all rows are new
        df_new = df

    ## Load data (append if table exists, create if not)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND" if table_exists else "WRITE_EMPTY",
        autodetect=True
    )

    job = client.load_table_from_dataframe(df_new, table_id, job_config=job_config)
    job.result()

    print(f"{len(df_new)} records loaded to BigQuery.")
    return table_id


## Connections Categorization Functions
def assign_charging_type(power_kw):
    types = []
    for p in power_kw:
        if p < 7:
            types.append("Slow")
        elif 7 <= p <= 22:
            types.append("Fast")
        else:
            types.append("Rapid")
    return types

def max_charging_type(types):
    if "Rapid" in types:
        return "Rapid"
    elif "Fast" in types:
        return "Fast"
    elif "Slow" in types:
        return "Slow"
    return np.nan

def extract_connections(conn_list):
    if not isinstance(conn_list, list):
        return {}

    ## Remove None values from arrays (BigQuery cannot accept nulls in arrays)
    powers = [c.get("PowerKW") for c in conn_list if c.get("PowerKW") is not None]
    conn_types = [c.get("ConnectionTypeID") for c in conn_list if c.get("ConnectionTypeID") is not None]
    current_types = [c.get("CurrentTypeID") for c in conn_list if c.get("CurrentTypeID") is not None]

    charging_type_list = assign_charging_type(powers) if powers else []

    return {
        "Number_of_Connectors": len(conn_list),
        "Power_List": powers or [],
        "Min_Power_kW": min(powers) if powers else None,
        "Max_Power_kW": max(powers) if powers else None,
        "Charging_Type_List": charging_type_list or [],
        "Max_Charging_Type": max_charging_type(charging_type_list),
        "Rapid_Charge_Available": "Yes" if "Rapid" in charging_type_list else "No",
        "Fast_Charge_Available": "Yes" if "Fast" in charging_type_list else "No",
        "Slow_Charge_Available": "Yes" if "Slow" in charging_type_list else "No",
        "Connection_Types": conn_types or [],
        "Connection_CurrentTypes": current_types or [],
    }

## Main cloud function
@functions_framework.http
def ocm_extractor(request):

    if not OCM_API_KEY:
        return ("ERROR: Missing OCM_API_KEY environment variable", 500)

    all_poi_data = []

    lng = MIN_LNG
    while lng < MAX_LNG:
        lat = MIN_LAT
        while lat < MAX_LAT:

            min_lat_box = lat
            max_lat_box = min(lat + STEP, MAX_LAT)
            min_lng_box = lng
            max_lng_box = min(lng + STEP, MAX_LNG)

            bbox_param = f"({min_lat_box},{min_lng_box}),({max_lat_box},{max_lng_box})"

            params = {
                "key": OCM_API_KEY,
                "boundingbox": bbox_param,
                "output": "json",
                "countrycode": "GB",
                "maxresults": 1000,
                "compact": "false",
                "verbose": "true",
            }

            print(f"Fetching: {bbox_param}")

            resp = safe_get(OCM_API_URL, params=params)

            if resp is None:
                print(f"Skipped bounding box: {bbox_param}")
            else:
                all_poi_data.extend(resp.json())

            time.sleep(1)
            lat += STEP
        lng += STEP

    if not all_poi_data:
        return ("No data returned from OCM API", 500)


    ## Data Cleaning Steps
    df = pd.DataFrame(all_poi_data)

    address_info = df["AddressInfo"].apply(pd.Series).add_prefix("Address_")

    ## Expand Nested Fields
    operator_info = (
        df["OperatorInfo"].apply(lambda x: x if isinstance(x, dict) else {})
        .apply(pd.Series).add_prefix("Operator_")
    )

    usage_type = (
        df["UsageType"].apply(lambda x: x if isinstance(x, dict) else {})
        .apply(pd.Series).add_prefix("Usage_")
    )

    status_type = (
        df["StatusType"].apply(lambda x: x if isinstance(x, dict) else {})
        .apply(pd.Series).add_prefix("Status_")
    )

    connection_info = df["Connections"].apply(extract_connections).apply(pd.Series)

    ## Combine all cleaned data
    df_final = pd.concat([
        df.drop(columns=[
            "AddressInfo", "Connections", "OperatorInfo", "UsageType", "StatusType"
        ], errors="ignore"),
        address_info, operator_info, usage_type, status_type, connection_info
    ], axis=1)

    ## Create Full Address Field
    df_final["Address"] = (
        df_final["Address_AddressLine1"].fillna("") + ", " +
        df_final["Address_Town"].fillna("") + ", " +
        df_final["Address_Postcode"].fillna("")
    ).str.replace(", ,", ", ").str.strip(", ")

    ## Rename Columns
    df_final = df_final.rename(columns={
        "ID": "Place_ID",
        "Operator_Title": "Operator",
        "Status_Title": "Bussiness_Status",
        "Address_Title": "Location_Name",
        "Address_Latitude": "Latitude",
        "Address_Longitude": "Longitude",
        "Usage_Title": "Usage",
    })

    ## Final Cleaning
    df_final['Operator'] = df_final['Operator'].replace({
        "(Business Owner at Location)": "Business Owner at Location",
        "(Unknown Operator)": "Unknown",
    })
    df_final['Usage'] = df_final['Usage'].replace({
        "(Unknown)": "Unknown",
    })
    df_final = df_final.dropna(subset=["Number_of_Connectors", "Min_Power_kW", "Max_Power_kW", "Latitude", "Longitude"], how='any')
    df_final = df_final.replace({None: np.nan})
    df_final = df_final.fillna("Unknown")
    df_final = df_final.replace(r'^\s*$', "Unknown", regex=True)

    selected_cols = [
        "Place_ID", "Operator", "Bussiness_Status", "Location_Name", "Address",
        "Latitude", "Longitude", "Usage","Number_of_Connectors", "Min_Power_kW", "Max_Power_kW",
        "Max_Charging_Type", "Rapid_Charge_Available", "Fast_Charge_Available", "Slow_Charge_Available"
    ]

    ## Handle empty Bussiness_Status
    df_final["Bussiness_Status"] = df_final["Bussiness_Status"].replace("", np.nan)
    df_final["Bussiness_Status"] = df_final["Bussiness_Status"].fillna("Unknown")
    df_final = df_final[[c for c in selected_cols if c in df_final.columns]]
    df_final = df_final.drop_duplicates(keep='last')

    ## Save CSV and load to BigQuery
    today_str = datetime.now().strftime("%Y_%m_%d")
    file_name = f"central_london_ocm_full_data_{today_str}.csv"
    csv_path = upload_to_gcs(df_final, file_name)
    bq_table = load_to_bigquery_incremental(df_final)

    return {
        "message": "OCM EV data extraction completed",
        "records": len(df_final),
        "csv_saved_to": csv_path,
        "bigquery_table": bq_table,
    }
