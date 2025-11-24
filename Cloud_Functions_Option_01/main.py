## Google Place API + OCM EV Data Extractor - Cloud Function
import datetime
import pandas as pd
import numpy as np
import requests
import time
import os
from google.cloud import storage
from google.cloud import bigquery
import functions_framework
from google.cloud.exceptions import NotFound

## Configurations
bucket_name = "ev-tracker-data-london-ocm-gpa"
project_name = "fiery-atlas-472112-s9"
dataset_name = "ev_data_ocm_gpa"
table_name = "ev_chargers_ocm_gpa_cleaned"

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
OCM_API_KEY = os.environ.get("OCM_API_KEY")

## URLs
NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
OCM_URL = "https://api.openchargemap.io/v3/poi/"

## Grid for Central London
MIN_LAT, MAX_LAT = 51.48, 51.55
MIN_LNG, MAX_LNG = -0.20, -0.02
SEARCH_RADIUS = 3000
OCM_DISTANCE_KM = 0.1

## EV Keyword Filter
CHARGER_KEYWORDS = [
    "charge", "charging", "ev", "ev charger", "ev-charger",
    "pod point", "bp pulse", "tesla", "supercharger",
    "instavolt", "chargepoint", "rapid charger",
    "shell recharge", "ionity", "mer", "octopus"
]
CHARGER_KEYWORDS = [k.lower() for k in CHARGER_KEYWORDS]


def debug_log(message):
    print(f"[DEBUG] {message}", flush=True)

## Charging Types Assignment
def assign_charging_type(power_list):
    types = []
    for p in power_list:
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
    if "Fast" in types:
        return "Fast"
    if "Slow" in types:
        return "Slow"
    return np.nan


## Google Nearby Search
def fetch_nearby(lat, lng):
    all_results = []
    pagetoken = None

    while True:
        params = {
            "location": f"{lat},{lng}",
            "radius": SEARCH_RADIUS,
            "keyword": "electric vehicle charging",
            "key": GOOGLE_API_KEY,
        }
        if pagetoken:
            params["pagetoken"] = pagetoken
            time.sleep(2)

        resp = requests.get(NEARBY_URL, params=params).json()
        all_results.extend(resp.get("results", []))
        pagetoken = resp.get("next_page_token")

        if not pagetoken:
            break

    return all_results



## Place Details + Keyword Validation
def fetch_place_details(place_id):
    params = {
        "place_id": place_id,
        "key": GOOGLE_API_KEY,
        "fields": "name,formatted_address,geometry,types,business_status,formatted_phone_number"
    }
    resp = requests.get(DETAILS_URL, params=params).json()
    return resp


def is_charger_place(details_json):
    result = details_json.get("result", {})

    ## Check Google 'types'
    types = result.get("types", [])
    if "electric_vehicle_charging_station" in types:
        return True

    ## Check name
    name = (result.get("name") or "").lower()
    if any(kw in name for kw in CHARGER_KEYWORDS):
        return True

    ## Check address
    addr = (result.get("formatted_address") or "").lower()
    if "charging" in addr or "ev" in addr:
        return True

    return False


## Open Charge Map (OCM) Details Fetcher
def fetch_ocm_details(lat, lng):

    if not OCM_API_KEY:
        return "Not Available", "Not Available"

    params = {
        "output": "json",
        "countrycode": "GB",
        "latitude": lat,
        "longitude": lng,
        "distance": OCM_DISTANCE_KM,
        "maxresults": 5,
        "key": OCM_API_KEY
    }

    try:
        resp = requests.get(OCM_URL, params=params)
        data = resp.json()

        if not data:
            return "No Match Found", "No Match Found"

        poi = data[0]
        connections = poi.get("Connections", [])
        if not connections:
            return "No Connections Listed", "No Connections Listed"

        connector_types = []
        power_values = []

        for conn in connections:
            conn_type = conn.get("ConnectionType", {}).get("Title", "Unknown")
            power_kw = conn.get("PowerKW")

            connector_types.append(conn_type)
            if power_kw:
                power_values.append(f"{float(power_kw):.1f} kW")
            else:
                power_values.append("Power Unknown")

        return "; ".join(connector_types), "; ".join(power_values)

    except Exception:
        return "OCM API Error", "OCM API Error"


## Extraction Grid
def run_extraction():
    records = []
    seen = set()

    lat_points = np.arange(MIN_LAT, MAX_LAT + 1e-9, 1)
    lng_points = np.arange(MIN_LNG, MAX_LNG + 1e-9, 1)

    for lat in lat_points:
        for lng in lng_points:

            results = fetch_nearby(lat, lng)
            for r in results:

                pid = r.get("place_id")
                if not pid or pid in seen:
                    continue

                details = fetch_place_details(pid)
                if not is_charger_place(details):
                    continue

                seen.add(pid)

                res = details.get("result", {})
                geom = res.get("geometry", {}).get("location", {})
                place_lat = geom.get("lat")
                place_lng = geom.get("lng")

                ocm_conns, ocm_power = fetch_ocm_details(place_lat, place_lng)

                records.append({
                    "place_id": pid,
                    "name": res.get("name"),
                    "address": res.get("formatted_address"),
                    "lat": place_lat,
                    "lng": place_lng,
                    "ocm_connector_types": ocm_conns,
                    "ocm_power_kW": ocm_power,
                    "types": ",".join(res.get("types", [])),
                    "business_status": res.get("business_status"),
                    "phone_number": res.get("formatted_phone_number"),
                })

    df = pd.DataFrame(records)
    return df


## Data Cleaning
def clean_ev_charger_data(df):

    df.drop_duplicates(subset=["place_id"], inplace=True)

    ## Remove invalid OCM rows
    bad = ["No Match Found", "No Connections Listed", "OCM API Error"]
    df = df[~df["ocm_connector_types"].isin(bad)]

    ## Split lists
    df["connector_list"] = df["ocm_connector_types"].str.split("; ")
    df["power_list"] = df["ocm_power_kW"].str.split("; ")

    ## Convert "50 kW" → float
    def convert_power(x):
        if not isinstance(x, list):
            return []
        out = []
        for p in x:
            if "kW" in p:
                out.append(float(p.replace(" kW", "")))
        return out

    df["power_list"] = df["power_list"].apply(convert_power)

    ## Charging types
    df["charging_type_list"] = df["power_list"].apply(assign_charging_type)
    df["Number_of_Connectors"] = df["connector_list"].apply(len)
    df["Max_Charging_Type"] = df["charging_type_list"].apply(max_charging_type)

    ## Min/max power
    df["Min_Power_kW"] = df["power_list"].apply(lambda x: min(x) if x else np.nan)
    df["Max_Power_kW"] = df["power_list"].apply(lambda x: max(x) if x else np.nan)

    ## Availability
    df["Rapid_Charge_Available"] = df["charging_type_list"].apply(lambda x: "Yes" if "Rapid" in x else "No")
    df["Fast_Charge_Available"]  = df["charging_type_list"].apply(lambda x: "Yes" if "Fast" in x else "No")
    df["Slow_Charge_Available"]  = df["charging_type_list"].apply(lambda x: "Yes" if "Slow" in x else "No")

    ## Drop temp columns
    df.drop(columns=["connector_list", "power_list", "charging_type_list", "types"], inplace=True)

    ## Rename column Names
    df.rename(columns={
        "place_id": "Place_Id",
        "name": "Location_Name",
        "address": "Address",
        "lat": "Latitude",
        "lng": "Longitude",
        "ocm_connector_types": "OCM_Connector_Types",
        "ocm_power_kW": "OCM_Power_kW",
        "business_status": "Business_Status",
        "phone_number": "Phone_Number"
    }, inplace=True)

    return df


## BigQuery Loader - Incremental No-Duplicates
def load_to_bigquery_incremental(df):
    client = bigquery.Client(project=project_name)
    table_id = f"{project_name}.{dataset_name}.{table_name}"
    df.rename(columns={"Place_Id": "Place_Id"}, inplace=True)

    try:
        client.get_table(table_id)
        table_exists = True
    except NotFound:
        print(f"Table {table_id} not found. Creating new table.")
        table_exists = False

    if table_exists:
        query = f"SELECT Place_Id FROM `{table_id}`"
        existing_ids = {row.Place_Id for row in client.query(query).result()}
        df_new = df[~df["Place_Id"].isin(existing_ids)]

        if df_new.empty:
            print("No new records to append.")
            return "No new data"
    else:
        df_new = df

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND" if table_exists else "WRITE_EMPTY",
        autodetect=True
    )

    job = client.load_table_from_dataframe(df_new, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df_new)} new records into BigQuery.")
    return "OK"


## ETL Process
def run_etl():

    df_raw = run_extraction()
    if df_raw.empty:
        return "No data extracted"

    df = clean_ev_charger_data(df_raw)

    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    csv_name = f"ev_data/ev_chargers_{today}.csv"

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(csv_name)
    blob.upload_from_string(df.to_csv(index=False))

    load_status = load_to_bigquery_incremental(df)

    return f"ETL DONE — {load_status}"



## Cloud Function Entry Point
@functions_framework.http
def ev_etl(request):
    try:
        result = run_etl()
        return {"status": "OK", "message": result}, 200
    except Exception as e:
        debug_log(f"ERROR: {str(e)}")
        return {"error": str(e)}, 500