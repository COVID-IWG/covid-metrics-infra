import pandas as pd
from google.cloud import storage
import matplotlib.pyplot as plt
import os
from datetime import date, timedelta

bucket_name = "daily_pipeline"
bucket = storage.Client().bucket(bucket_name)

state_lookup = {
 'Andaman and Nicobar Islands': 'AN',
 'Andhra Pradesh': 'AP',
 'Arunachal Pradesh': 'AR',
 'Assam': 'AS',
 'Bihar': 'BR',
 'Chandigarh': 'CH',
 'Chhattisgarh': 'CT',
 'Daman and Diu': 'DD',
 'Dadra and Nagar Haveli and Daman and Diu': 'DDDN',
 'Delhi': 'DL',
 'Dadra and Nagar Haveli': 'DN',
 'Goa': 'GA',
 'Gujarat': 'GJ',
 'Himachal Pradesh': 'HP',
 'Haryana': 'HR',
 'Jharkhand': 'JH',
 'Jammu and Kashmir': 'JK',
 'Karnataka': 'KA',
 'Kerala': 'KL',
 'Ladakh': 'LA',
 'Lakshadweep': 'LD',
 'Maharashtra': 'MH',
 'Meghalaya': 'ML',
 'Manipur': 'MN',
 'Madhya Pradesh': 'MP',
 'Mizoram': 'MZ',
 'Nagaland': 'NL',
 'Odisha': 'OR',
 'Punjab': 'PB',
 'Puducherry': 'PY',
 'Rajasthan': 'RJ',
 'Sikkim': 'SK',
 'Telangana': 'TG',
 'Tamil Nadu': 'TN',
 'Tripura': 'TR',
 'India': 'TT',
 'State Unassigned': 'UN',
 'Uttar Pradesh': 'UP',
 'Uttarakhand': 'UT',
 'West Bengal': 'WB'}

def transfer_image_to_bucket(file_path):
    size_kb = os.stat(file_path).st_size / 1000
    print("Timeseries artifact size : {} KB".format(size_kb))
    assert size_kb > 15
    file_name = file_path.split("/")[2]
    bucket.blob("pipeline/rpt/{}".format(file_name)).upload_from_filename(file_path, content_type="image/png")


def transfer_csv_to_bucket(file_path):
    file_name = file_path.split("/")[2]
    bucket.blob("pipeline/rpt/{}".format(file_name)).upload_from_filename(file_path)

def obtain_from_bucket(file_name):
    bucket.blob("pipeline/data/{}".format(file_name)).download_to_filename("/tmp/{}".format(file_name))
    return "/tmp/{}".format(file_name)

def generate_vax_report(_):

    # URL of the csv used directly

    df = pd.read_csv("https://www.dropbox.com/sh/y949ncp39towulf/AACd3YxzfB0fHkjQ1YJG-W2ba/covid/csv/covid_vaccination.csv?dl=1")

    # convert date to proper format
    df["date"] = pd.to_datetime(df["date"])
    df["district_state"] = df["district"] + ", " + df["state"].apply(lambda x : state_lookup[x])

    statesList = df["state"].unique()
    for state in statesList:
        stateDF = df.loc[df["state"] == state]

        # state level total aggregates
        totalStateDailyAgg = stateDF.groupby("date").sum()
        state_code = state_lookup[state]
        totalStateDailyAgg["first_dose_admin"].plot()
        plt.title("Number of first doses administered - {}".format(state))
        plt.savefig("/tmp/first_dose_admin_{}.png".format(state_code))
        plt.close()
        print("Generated first dose statistics plot for {}".format(state))

        totalStateDailyAgg["total_individuals_registered"].plot()
        plt.title("Number of individuals registered - {}".format(state))
        plt.savefig("/tmp/total_individuals_registered_{}.png".format(state_code))
        plt.close()
        print("Generated total individuals registered plot for {}".format(state))

        totalStateDailyAgg["second_dose_admin"].plot()
        plt.title("Number of second doses administered - {}".format(state))
        plt.savefig("/tmp/second_dose_admin_{}.png".format(state_code))
        plt.close()
        print("Generated second dose statistics plot for {}".format(state))

        # Check if the outputs are at least 50 kb and transfer them to buckets
        transfer_image_to_bucket("/tmp/first_dose_admin_{}.png".format(state_code))
        transfer_image_to_bucket("/tmp/total_individuals_registered_{}.png".format(state_code))
        transfer_image_to_bucket("/tmp/second_dose_admin_{}.png".format(state_code))

    # top 10 districts nationwide based on number of vaccines administered in a given day

    df["total_vac"] = df["male_vac"] + df["female_vac"] + df["trans_vac"]

    today = df["date"].sort_values().values[-1] # Cheeky way to get the last date in the DF
    today = pd.to_datetime(today)
    yesterday = today - timedelta(days=1)

    todayDF = df.loc[df["date"] == today]
    yesterdayDF = df.loc[df["date"] == yesterday]
    differenceDF = todayDF.groupby("district_state").sum()["first_dose_admin"] + todayDF.groupby("district_state").sum()["second_dose_admin"] - yesterdayDF.groupby("district_state").sum()["first_dose_admin"] - yesterdayDF.groupby("district_state").sum()["second_dose_admin"]

    differenceDF.sort_values(ascending=False).to_csv("/tmp/districts_sorted_absolute.csv")
    transfer_csv_to_bucket("/tmp/districts_sorted_absolute.csv")

    # population stuff

    vaccine_eligible_pop = pd.read_csv(obtain_from_bucket("vaccine_eligible_pop_state_wise.csv"))

    stateWiseVax = todayDF.groupby("lgd_state_name").sum()
    indiaVax = pd.DataFrame(stateWiseVax.sum(axis=0)).T
    indiaVax.index = ["india"]
    stateWiseVax = pd.concat([stateWiseVax, indiaVax])
    stateWisePop = vaccine_eligible_pop.set_index("State")
    percentageFirstDose = (stateWiseVax["first_dose_admin"])/stateWisePop["20+ total"] * 100.0
    percentageSecondDose = (stateWiseVax["second_dose_admin"]/stateWisePop["20+ total"]) * 100.0
    percentageOverall = (stateWiseVax["first_dose_admin"] + stateWiseVax["second_dose_admin"])/stateWisePop["20+ total"] * 100.0

    percentageFirstDose.sort_values(ascending=False).to_csv("/tmp/percentage_first_dose_state_wise.csv")
    percentageSecondDose.sort_values(ascending=False).to_csv("/tmp/percentage_second_dose_state_wise.csv")
    percentageOverall.sort_values(ascending=False).to_csv("/tmp/percentage_overall_state_wise.csv")

    with open("/tmp/today.txt", "w") as f:
        f.write(str(today))
    transfer_csv_to_bucket("/tmp/today.txt")
    transfer_csv_to_bucket("/tmp/percentage_first_dose_state_wise.csv")
    transfer_csv_to_bucket("/tmp/percentage_second_dose_state_wise.csv")
    transfer_csv_to_bucket("/tmp/percentage_overall_state_wise.csv")

    return "OK!"
