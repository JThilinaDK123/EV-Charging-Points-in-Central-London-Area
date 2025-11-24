import pandas as pd
import numpy as np

## Classify charging type based on power
def assign_charging_type(power):
    types = []
    for p in power:
        if p < 7:
            types.append('Slow')
        elif 7 <= p <= 22:
            types.append('Fast')
        else:
            types.append('Rapid')
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

## Main data cleaning function
def clean_ev_charger_data():

    df = pd.read_csv("GPA_OCM_Extracted_Data/central_london_gpa_ocm_extracted_data.csv")

    ## Drop unnecessary columns
    df = df.drop(columns=['Types'])
    df.drop_duplicates(inplace=True)
    
    ## Remove entries with no OCM connector types
    df = df[df['ocm_connector_types'] != 'No Match Found']
    df = df[df['ocm_connector_types'] != 'No Connections Listed']
    
    ## Split connector types and power
    df['connector_list'] = df['ocm_connector_types'].str.split('; ')
    df['power_list'] = df['ocm_power_kW'].str.split('; ')

    ## Clean power list -> convert "50 kW" → 50.0
    df['power_list'] = df['power_list'].apply(
        lambda x: [float(p.replace(' kW', '')) for p in x] if isinstance(x, list) else []
    )

    ## Assign charging types per connector
    df['charging_type_list'] = df['power_list'].apply(assign_charging_type)

    ## Number of connectors
    df['Number_of_Connectors'] = df['connector_list'].apply(len)

    ## Max charging type for site
    df['Max_Charging_Type'] = df['charging_type_list'].apply(max_charging_type)

    ## Min and Max power
    df['Min_Power_kW'] = df['power_list'].apply(lambda x: min(x) if len(x) > 0 else np.nan)
    df['Max_Power_kW'] = df['power_list'].apply(lambda x: max(x) if len(x) > 0 else np.nan)


    ## Add availability columns
    df['Rapid_Charge_Available'] = df['charging_type_list'].apply(lambda x: 'Yes' if 'Rapid' in x else 'No')
    df['Fast_Charge_Available']  = df['charging_type_list'].apply(lambda x: 'Yes' if 'Fast' in x else 'No')
    df['Slow_Charge_Available']  = df['charging_type_list'].apply(lambda x: 'Yes' if 'Slow' in x else 'No')

    # Drop unused columns
    df = df.drop(columns=['ocm_connector_types', 'ocm_power_kW', 'connector_list', 'power_list', 'charging_type_list'])
    df.to_csv("GPA_OCM_Extracted_Data/central_london_gpa_ocm_final_data.csv", index=False)

    return df

if __name__ == "__main__":
    df = clean_ev_charger_data()
