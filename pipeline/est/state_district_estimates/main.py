import traceback

import pandas as pd
from epimargin.estimators import analytical_MPVS
from epimargin.etl.covid19india import state_code_lookup
from epimargin.smoothing import notched_smoothing
from google.cloud import storage

# model details 
gamma     = 0.1 # 10 day infectious period
smoothing = 10
CI        = 0.95
lookback  = 120 # how many days back to start estimation
cutoff    = 2   # most recent data to use 
dissolved_states = ["Delhi", "Chandigarh", "Manipur", "Sikkim", "Dadra And Nagar Haveli And Daman And Diu", "Andaman And Nicobar Islands", "Telangana", "Goa", "Assam", "Lakshadweep"]
excluded = ["Unknown", "Other State", "Other Region", "Airport Quarantine", "Railway Quarantine", "BSF Camp", "Foreign Evacuees", "Italians", "Evacuees"]

# cloud details 
bucket_name = "daily_pipeline"



def get(request, key):
    request_json = request.get_json()
    if request.args and key in request.args:
        return request.args.get(key)
    elif request_json and key in request_json:
        return request_json[key]
    else:
        return None

def run_estimates(request):
    state_code = get(request, 'state_code')
    state = state_code_lookup[state_code]

    print(f"Rt estimation for {state} ({state_code}) started")
    
    bucket = storage.Client().bucket(bucket_name)
    bucket.blob("pipeline/commons/refs/all_crosswalk.dta")\
        .download_to_filename("/tmp/all_crosswalk.dta")

    bucket.blob("pipeline/raw/states.csv")\
        .download_to_filename("/tmp/states.csv")

    bucket.blob("pipeline/raw/districts.csv")\
        .download_to_filename("/tmp/districts.csv")

    crosswalk   = pd.read_stata("/tmp/all_crosswalk.dta")
    district_cases = pd.read_csv("/tmp/districts.csv")\
        .rename(columns = str.lower)\
        .set_index(["state", "district", "date"])\
        .sort_index()\
        .rename(index = lambda s: s.replace(" and ", " & "), level = 0)\
        .loc[state]
    state_cases = pd.read_csv("/tmp/states.csv")\
        .rename(columns = str.lower)\
        .set_index(["state", "date"])\
        .sort_index()\
        .rename(index = lambda s: s.replace(" and ", " & "), level = 0)\
        .loc[state]
    print(f"Estimating state-level Rt for {state_code}") 
    normalized_state = state.replace(" and ", " And ").replace(" & ", " And ")
    lgd_state_name, lgd_state_id = crosswalk.query("state_api == @normalized_state").filter(like = "lgd_state").drop_duplicates().iloc[0]
    try:
        (
            dates,
            Rt_pred, Rt_CI_upper, Rt_CI_lower,
            T_pred, T_CI_upper, T_CI_lower,
            total_cases, new_cases_ts, *_
        ) = analytical_MPVS(
            state_cases.iloc[-lookback:-cutoff].confirmed, 
            CI = CI, smoothing = notched_smoothing(window = smoothing), totals = True
        )

        pd.DataFrame(data = {
            "dates": dates[1:],
            "Rt_pred": Rt_pred,
            "Rt_CI_upper": Rt_CI_upper,
            "Rt_CI_lower": Rt_CI_lower,
            "T_pred": T_pred,
            "T_CI_upper": T_CI_upper,
            "T_CI_lower": T_CI_lower,
            "total_cases": total_cases[2:],
            "new_cases_ts": new_cases_ts,
        })\
            .assign(state = state, lgd_state_name = lgd_state_name, lgd_state_id = lgd_state_id)\
            .to_csv("/tmp/state_Rt.csv")

        # upload to cloud
        bucket.blob(f"pipeline/est/{state_code}_state_Rt.csv").upload_from_filename("/tmp/state_Rt.csv",    content_type = "text/csv")
    except Exception as e:
        print(f"ERROR when estimating Rt for {state_code}", e)
        print(traceback.print_exc())
    
    if normalized_state in dissolved_states:
        print(f"Skipping district-level Rt for {state_code}")
    else:
        print(f"Estimating district-level Rt for {state} ({state_code})")
        estimates = []
        for district in filter(lambda _: _.strip() not in excluded, district_cases.index.get_level_values(0).unique()):
            print(f"running estimation for [{district}]")
            lgd_district_data = crosswalk.query("state_api == @normalized_state & district_api == @district").filter(like = "lgd_district").drop_duplicates()
            if not lgd_district_data.empty:
                lgd_district_name, lgd_district_id = lgd_district_data.iloc[0]
            else:
                lgd_district_name, lgd_district_id = lgd_state_name, lgd_state_id
            try:
                (
                    dates,
                    Rt_pred, Rt_CI_upper, Rt_CI_lower,
                    T_pred, T_CI_upper, T_CI_lower,
                    total_cases, new_cases_ts, *_
                ) = analytical_MPVS(district_cases.loc[district].iloc[-lookback:-cutoff].confirmed, CI = CI, smoothing = notched_smoothing(window = smoothing), totals = True)
                estimates.append(pd.DataFrame(data = {
                    "dates": dates[1:],
                    "Rt_pred": Rt_pred,
                    "Rt_CI_upper": Rt_CI_upper,
                    "Rt_CI_lower": Rt_CI_lower,
                    "T_pred": T_pred,
                    "T_CI_upper": T_CI_upper,
                    "T_CI_lower": T_CI_lower,
                    "total_cases": total_cases[2:],
                    "new_cases_ts": new_cases_ts,
                }).assign(state = state, lgd_state_name = lgd_state_name, lgd_state_id = lgd_state_id, district = district, lgd_district_name = lgd_district_name, lgd_district_id = lgd_district_id))
            except Exception as e:
                print(f"ERROR when estimating Rt for {district}, {state_code}", e)
                print(traceback.print_exc())

        pd.concat(estimates).to_csv("/tmp/district_Rt.csv")
    
        # upload to cloud
        bucket.blob(f"pipeline/est/{state_code}_district_Rt.csv").upload_from_filename("/tmp/district_Rt.csv", content_type = "text/csv")
    
    return "OK!"
