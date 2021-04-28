import traceback

import numpy as np
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
excluded = ["Unknown", "Other State", "Airport Quarantine", "Railway Quarantine"]


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

    print(f"Rt estimation for {state_code} ({state}) started")
    
    bucket = storage.Client().bucket(bucket_name)
    bucket.blob("pipeline/raw/state_case_timeseries.csv")\
        .download_to_filename("/tmp/state_case_timeseries.csv")

    bucket.blob("pipeline/raw/district_case_timeseries.csv")\
        .download_to_filename("/tmp/district_case_timeseries.csv")

    state_ts    = pd.read_csv("/tmp/state_case_timeseries.csv")   .set_index(["detected_state"])
    district_ts = pd.read_csv("/tmp/district_case_timeseries.csv").set_index(["detected_state", "detected_district"]).loc[state]

    print(f"Estimating state-level Rt for {state_code}") 
    (
        dates,
        Rt_pred, Rt_CI_upper, Rt_CI_lower,
        T_pred, T_CI_upper, T_CI_lower,
        total_cases, new_cases_ts,
        anomalies, anomaly_dates
    ) = analytical_MPVS(
        state_ts.loc[state].set_index("status_change_date").iloc[-lookback:-cutoff].Hospitalized, 
        CI = CI, smoothing = notched_smoothing(window = smoothing), totals = False
    )
    
    pd.DataFrame(data = {
        "dates": dates,
        "Rt_pred": Rt_pred,
        "Rt_CI_upper": Rt_CI_upper,
        "Rt_CI_lower": Rt_CI_lower,
        "T_pred": T_pred,
        "T_CI_upper": T_CI_upper,
        "T_CI_lower": T_CI_lower,
        "total_cases": total_cases[2:],
        "new_cases_ts": new_cases_ts,
    }).to_csv("/tmp/state_Rt.csv")

    print(f"Estimating district-level Rt for {state_code}")
    district_Rt = {}
    estimates = []
    for district in filter(lambda _: _ not in excluded, district_ts.index.get_level_values(0).unique()):
        try:
            (
                dates,
                Rt_pred, Rt_CI_upper, Rt_CI_lower,
                T_pred, T_CI_upper, T_CI_lower,
                total_cases, new_cases_ts,
                anomalies, anomaly_dates
            ) = analytical_MPVS(district_ts.loc[district].set_index("status_change_date").iloc[-lookback:-cutoff].Hospitalized, CI = CI, smoothing = notched_smoothing(window = smoothing), totals = False)
            district_Rt[district] = Rt_pred[-1]
            estimates.append(pd.DataFrame(data = {
                "dates": dates,
                "Rt_pred": Rt_pred,
                "Rt_CI_upper": Rt_CI_upper,
                "Rt_CI_lower": Rt_CI_lower,
                "T_pred": T_pred,
                "T_CI_upper": T_CI_upper,
                "T_CI_lower": T_CI_lower,
                "total_cases": total_cases[2:],
                "new_cases_ts": new_cases_ts,
            }).assign(district = district))
        except Exception as e:
            print(f"ERROR when estimating Rt for {district}, {state_code}", e)
            print(traceback.print_exc())
            district_Rt[district] = np.nan

    pd.concat(estimates).to_csv("/tmp/district_Rt.csv")
    top10 = {k: f"{v:.2f}" for (k, v) in sorted(district_Rt.items(), key = lambda t:t[1], reverse = True)[:10]}
    pd.DataFrame.from_dict(top10, orient = "index", columns = ["Rt"]).to_csv("/tmp/top10.csv")
    
    # upload to cloud
    bucket.blob(f"pipeline/est/{state_code}_state_Rt.csv")      .upload_from_filename("/tmp/state_Rt.csv",    content_type = "text/csv")
    bucket.blob(f"pipeline/est/{state_code}_district_Rt.csv")   .upload_from_filename("/tmp/district_Rt.csv", content_type = "text/csv")
    bucket.blob(f"pipeline/est/{state_code}_top10_district.csv").upload_from_filename("/tmp/top10.csv",       content_type = "text/csv")

    return "OK!"