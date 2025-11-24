import requests
import pandas as pd
import time
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()
GOOGLE_API_KEY = os.getenv("API_KEY")
OCM_API_KEY = os.getenv("OCM_API_KEY").strip()

## Google Places API Constants
QUERY_KEYWORD = "electric vehicle charging"
QUERY_TYPE = "electric_vehicle_charging_station"
NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

## Open Charge Map (OCM) API Constants 
OCM_URL = "https://api.openchargemap.io/v3/poi/"
OCM_DISTANCE_KM = 0.1 

## Central London small bounding box (Used for grid search)
MIN_LAT, MAX_LAT = 51.48, 51.55
MIN_LNG, MAX_LNG = -0.20, -0.02
SEARCH_RADIUS = 3000   # meters 

## Keywords used by EV charging points
CHARGER_KEYWORDS = [
    "charge", "charging", "ev", "ev-charger", "ev charger", "pod point", "pod-point",
    "bp pulse", "bp-pulse", "tesla", "tesla supercharger", "instavolt",
    "chargepoint", "rapid charger", "rapid charge", "ac charger", "dc charger",
    "shell recharge", "shell-recharge", "ionity", "evgo", "engie", "mer",
    "octopus", "octopus energy", "podpoint"
]
CHARGER_KEYWORDS = [k.lower() for k in CHARGER_KEYWORDS]


## Verify if the place is an EV charger
def is_charger_place(details_json):
    ## Check types returned by Place Details
    types = details_json.get("result", {}).get("types", []) or []
    types = [t.lower() for t in types]
    if "electric_vehicle_charging_station" in types or "charging_station" in types:
        return True

    ## Check name for common keywords
    name = details_json.get("result", {}).get("name", "") or ""
    name_l = name.lower()
    for kw in CHARGER_KEYWORDS:
        if kw in name_l:
            return True

    ## Check the address or business_status that contains 'charging'
    addr = details_json.get("result", {}).get("formatted_address", "") or ""
    if "charging" in addr.lower() or "ev" in addr.lower():
        return True

    return False

## Nearby Search
def fetch_nearby(lat, lng):
    all_results = []
    pagetoken = None

    while True:
        params = {
            "location": f"{lat},{lng}",
            "radius": SEARCH_RADIUS,
            "keyword": QUERY_KEYWORD,
            "key": GOOGLE_API_KEY,
        }
        if pagetoken:
            params["pagetoken"] = pagetoken
            time.sleep(2.2)

        resp = requests.get(NEARBY_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        all_results.extend(data.get("results", []))

        pagetoken = data.get("next_page_token")
        if not pagetoken:
            break

    return all_results


def fetch_place_details(place_id, fields="name,formatted_address,geometry,types,business_status,permanently_closed,formatted_phone_number"):

    params = {
        "place_id": place_id,
        "key": GOOGLE_API_KEY,
        "fields": fields
    }
    resp = requests.get(DETAILS_URL, params=params)
    resp.raise_for_status()
    return resp.json()


## Open Charge Map API
## Queries the Open Charge Map API for connector type and power at the given latitude and longitude
def fetch_ocm_details(lat, lng):

    if not OCM_API_KEY:
        print("OCM_API_KEY is not set. Skipping OCM search.")
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
        resp.raise_for_status()
        data = resp.json()
        
        if not data:
            return "No Match Found", "No Match Found"
        
        ## It assumes that the closest or first result is the correct match
        poi = data[0]
        connections = poi.get("Connections", [])
        
        if not connections:
            return "No Connections Listed", "No Connections Listed"

        ## Consolidate connector details and power
        connector_types = []
        power_kWs = []
        
        for conn in connections:
            conn_type = conn.get("ConnectionType", {}).get("Title", "Unknown Type")
            power_kw = conn.get("PowerKW")
            
            if power_kw is not None:
                connector_types.append(f"{conn_type}")
                power_kWs.append(f"{float(power_kw):.1f} kW")
            else:
                connector_types.append(f"{conn_type}")
                power_kWs.append("Power Unknown")

        return "; ".join(connector_types), "; ".join(power_kWs)

    except Exception as e:
        print(f"OCM search error for {lat}, {lng}: {e}")
        return "OCM API Error", "OCM API Error"


def fetch_central_london_verified():
    
    records = []
    seen = set()

    ## Create a grid of points to scan
    lat_points = np.arange(MIN_LAT, MAX_LAT + 1e-9, 0.3)
    lng_points = np.arange(MIN_LNG, MAX_LNG + 1e-9, 0.3)

    print(f"Scanning grid of {len(lat_points) * len(lng_points)} points.")

    for lat in lat_points:
        for lng in lng_points:
            print(f"\nScanning Google Nearby Search at grid {lat:.4f}, {lng:.4f} ...")
            try:
                results = fetch_nearby(lat, lng)
            except Exception as e:
                print("Nearby search error:", e)
                continue

            for r in results:
                pid = r.get("place_id")
                if not pid or pid in seen:
                    continue
                
                ## Fetch Google Place Details
                try:
                    details = fetch_place_details(pid)
                    time.sleep(0.35)
                except Exception as e:
                    print(f"Details fetch error for {pid}: {e}")
                    time.sleep(1)
                    continue

                if not is_charger_place(details):
                    continue

                seen.add(pid)

                res = details.get("result", {})
                geom = res.get("geometry", {}).get("location", {})
                place_lat = geom.get("lat") or r.get("geometry", {}).get("location", {}).get("lat")
                place_lng = geom.get("lng") or r.get("geometry", {}).get("location", {}).get("lng")
                
                ## Fetch OCM Details using coordinates from Google
                print(f"-> Found {res.get('name')}. Querying OCM...")
                ocm_connectors, ocm_power = fetch_ocm_details(place_lat, place_lng)
                time.sleep(0.2)

                ## Append combined record
                records.append({
                    "Place_ID": pid,
                    "Name": res.get("name"),
                    "Address": res.get("formatted_address"),
                    "Latitude": place_lat,
                    "Longitude": place_lng,
                    "ocm_connector_types": ocm_connectors,
                    "ocm_power_kW": ocm_power,            
                    "Types": ",".join(res.get("types", [])),
                    "Bussiness_Status": res.get("business_status"),
                    "Phone_Number": res.get("formatted_phone_number"),
                })

    df = pd.DataFrame(records)
    df.to_csv("GPA_OCM_Extracted_Data/central_london_gpa_ocm_extracted_data.csv", index=False)
    return df

if __name__ == "__main__":
    df = fetch_central_london_verified()

