import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()
OCM_API_KEY = os.getenv("OCM_API_KEY").strip()

## Define bounding box for Central London
MIN_LAT, MAX_LAT = 51.48, 51.55
MIN_LNG, MAX_LNG = -0.20, -0.02
STEP = 0.01

## Data Extraction from Open Charge Map (OCM) API
OCM_API_URL = "https://api.openchargemap.io/v3/poi/"
all_poi_data = []

print(f"Starting OCM data extraction for bounding box: ({MIN_LAT}, {MIN_LNG}) to ({MAX_LAT}, {MAX_LNG})")

## Extract data in sub-boxes
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
            'key': OCM_API_KEY,
            'boundingbox': bbox_param,
            'output': 'json',
            'countrycode': 'GB',
            'maxresults': 1000,
            'compact': 'false',
            'verbose': 'true'
        }

        try:
            response = requests.get(OCM_API_URL, params=params)
            response.raise_for_status()
            poi_list = response.json()
            all_poi_data.extend(poi_list)

            print(f"Extracted {len(poi_list)} POIs for sub-box: {bbox_param}")

        except Exception as e:
            print(f"Error fetching {bbox_param}: {e}")

        lat += STEP

    lng += STEP


print("\n--- Extraction Complete ---")
print(f"Total POIs Extracted: {len(all_poi_data)}")

if not all_poi_data:
    print("No data retrieved. Check API key or network.")
    exit()

df = pd.DataFrame(all_poi_data)


## AddressInfo
address_info = df['AddressInfo'].apply(pd.Series).add_prefix("Address_")

## OperatorInfo
operator_info = df['OperatorInfo'].apply(
    lambda x: x if isinstance(x, dict) else {}
).apply(pd.Series).add_prefix("Operator_")

## UsageType 
usage_type = df['UsageType'].apply(
    lambda x: x if isinstance(x, dict) else {}
).apply(pd.Series).add_prefix("Usage_")

## StatusType 
status_type = df['StatusType'].apply(
    lambda x: x if isinstance(x, dict) else {}
).apply(pd.Series).add_prefix("Status_")

## Connections and Charging Types
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
    if 'Rapid' in types:
        return 'Rapid'
    elif 'Fast' in types:
        return 'Fast'
    elif 'Slow' in types:
        return 'Slow'
    else:
        return np.nan

## Connections
def extract_connections(conn_list):
    if not isinstance(conn_list, list):
        return {}

    powers = [c.get("PowerKW") for c in conn_list if c.get("PowerKW") is not None]
    conn_types = [c.get("ConnectionTypeID") for c in conn_list]
    current_types = [c.get("CurrentTypeID") for c in conn_list]

    ## Charging type per connector
    charging_type_list = assign_charging_type(powers) if powers else []

    return {
        "Number_of_Connectors": len(conn_list),
        "Power_List": powers,
        "Min_Power_kW": min(powers) if powers else None,
        "Max_Power_kW": max(powers) if powers else None,
        "Charging_Type_List": charging_type_list,
        "Max_Charging_Type": max_charging_type(charging_type_list),
        "Rapid_Charge_Available": "Yes" if "Rapid" in charging_type_list else "No",
        "Fast_Charge_Available": "Yes" if "Fast" in charging_type_list else "No",
        "Slow_Charge_Available": "Yes" if "Slow" in charging_type_list else "No",
        "Connection_Types": conn_types,
        "Connection_CurrentTypes": current_types,
    }

connection_info = df["Connections"].apply(extract_connections).apply(pd.Series)

## Combine all extracted and transformed data
df_final = pd.concat([
    df.drop(columns=[
        'AddressInfo', 'Connections', 'OperatorInfo', 'UsageType', 'StatusType'
    ], errors='ignore'),
    address_info,
    operator_info,
    usage_type,
    status_type,
    connection_info
], axis=1)

## Data Cleaning and Transformation
df_final["Address"] = (
    df_final["Address_AddressLine1"].fillna("") + ", " +
    df_final["Address_Town"].fillna("") + ", " +
    df_final["Address_Postcode"].fillna("")
).str.replace(", ,", ", ").str.strip(", ")

df_final = df_final.rename(columns={
    "ID": "Place_ID",
    "Operator_Title": "Operator",
    "Status_Title": "Bussiness_Status",
    "Address_Title": "Location_Name",
    "Address_Latitude": "Latitude",
    "Address_Longitude": "Longitude",
    "Usage_Title": "Usage",
})

df_final['Operator'] = df_final['Operator'].replace({
    "(Business Owner at Location)": "Business Owner at Location",
    "(Unknown Operator)": "Unknown",
})
df_final['Usage'] = df_final['Usage'].replace({
    "(Unknown)": "Unknown",
})
df_final = df_final.dropna(subset=["Number_of_Connectors", "Min_Power_kW",
                                   "Max_Power_kW", "Latitude", "Longitude"], how='any')
df_final = df_final.replace({None: np.nan})
df_final = df_final.fillna("Unknown")
df_final = df_final.replace(r'^\s*$', "Unknown", regex=True)

selected_cols = [
    "Place_ID", "Operator", "Bussiness_Status", "Location_Name", "Address",
    "Latitude", "Longitude", "Usage","Number_of_Connectors", "Min_Power_kW", "Max_Power_kW",
    "Max_Charging_Type", "Rapid_Charge_Available", "Fast_Charge_Available", "Slow_Charge_Available"
]

df_final["Bussiness_Status"] = df_final["Bussiness_Status"].replace("", np.nan)
df_final["Bussiness_Status"] = df_final["Bussiness_Status"].fillna("Unknown")

df_export = df_final[[c for c in selected_cols if c in df_final.columns]]
df_export.to_csv("OCM_Extracted_Data/central_london_ocm_final_data.csv", index=False)
