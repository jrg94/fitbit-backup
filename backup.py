import logging
import os
import shutil
import tempfile
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import dotenv
import fitbit
import pandas as pd
from git import Repo

log = logging.getLogger(__name__)


def refresh_cb(token: dict) -> None:
    """
    Provides a mechanism for updating the Fitbit API tokens.

    :param token: a dictionary of token data
    """
    if os.environ.get("FITBIT_ACCESS_TOKEN") != token["access_token"]:
        log.info("Updating FITBIT_ACCESS_TOKEN.")
        dotenv.set_key(".env", "FITBIT_ACCESS_TOKEN", token["access_token"])
    if os.environ.get("FITBIT_REFRESH_TOKEN") != token["refresh_token"]:
        log.info("Updating FITBIT_REFRESH_TOKEN.")
        dotenv.set_key(".env", "FITBIT_REFRESH_TOKEN", token["refresh_token"])
    dotenv.set_key(".env", "FITBIT_EXPIRES_AT", str(token["expires_at"]))


def commit_csv() -> None:
    """
    Commits the fitbit data to the personal data repo.
    """
    with tempfile.TemporaryDirectory() as dir:
        repo = Repo.clone_from(
            "https://github.com/jrg94/personal-data.git", dir)
        health_data_path = Path(dir) / "health"
        shutil.copyfile("data/fitbit.csv",
                        str(health_data_path / "fitbit.csv"))
        repo.index.add([str(health_data_path / "fitbit.csv")])
        commit = repo.index.commit(f"Updated fitbit data automatically")
        if not commit.stats.files:
            log.info("No changes to commit.")
        else:
            log.info(f"Committing changes: {commit.stats.files}")
            repo.remote(name="origin").push()
        repo.close()
        
        
def get_row_of_data(date: str) -> tuple[pd.DataFrame, int]:
    """
    Grabs a day's worth of data from the Fitbit API.
    
    :param date: the date to pull data for
    :return: a dataframe of the data and the number of requests to generate a row
    """
    to_df = {}
    columns = {
        "dateTime": "date", 
        "value": "Steps", 
        "bmi": "BMI", 
        "fat": "Body Fat %", 
        "weight": "Weight", 
        "totalMinutesAsleep": "Total Sleep (minutes)",
        "totalSleepRecords": "Total Sleep Records",
        "totalTimeInBed": "Total Time in Bed (minutes)",
    }
    
    day_of_sleep: dict = client.sleep(date)["summary"]
    day_of_sleep.pop("stages")
    to_df |= day_of_sleep
    
    day_of_steps: dict = client.time_series("activities/steps", base_date=date, period="1d")["activities-steps"][0]
    to_df |= day_of_steps
    
    day_of_body: dict = client.body(date)["body"]
    # TODO: deal with zero values
    to_df |= day_of_body
    
    df = pd.DataFrame(to_df, index=[date])
    df.rename(columns=columns, inplace=True)
    log.info(f"Collected a row of data:\n{df}")
    return df, 3


def get_latest_data():
    """
    Takes the existing data and updates it with the latest data.
    
    :return: a dataframe of the complete data set
    """
    requests = 0
    df = pd.read_csv("https://raw.githubusercontent.com/jrg94/personal-data/main/health/fitbit.csv")
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)
    while (latest_date := df.index.max()) < datetime.today():
        date_range = pd.date_range(latest_date, periods=2, freq="D", inclusive="right")
        for date in date_range:
            row, curr = get_row_of_data(date)
            requests += curr
            pd.concat([df, row])
    return df, requests


if __name__ == "__main__":
    log_path = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        "logs",
        "fitbit.log"
    )
    logging.basicConfig(
        handlers=[RotatingFileHandler(
            log_path, backupCount=10, maxBytes=1000000)],
        level=logging.DEBUG,
        format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d:%H:%M:%S',
    )

    # Load the .env file
    log.info("Loading .env file.")
    dotenv.load_dotenv()

    # Parse out the tokens from the .env file
    log.info("Parsing tokens from .env file.")
    CLIENT_ID = os.environ.get("FITBIT_CLIENT_ID")
    CLIENT_SECRET = os.environ.get("FITBIT_CLIENT_SECRET")
    ACCESS_TOKEN = os.environ.get("FITBIT_ACCESS_TOKEN")
    REFRESH_TOKEN = os.environ.get("FITBIT_REFRESH_TOKEN")
    EXPIRES_AT = os.environ.get("FITBIT_EXPIRES_AT")

    # Initiate the Fitbit API
    log.info("Initiating Fitbit API.")
    client = fitbit.Fitbit(
        CLIENT_ID,
        CLIENT_SECRET,
        access_token=ACCESS_TOKEN,
        refresh_token=REFRESH_TOKEN,
        refresh_cb=refresh_cb
    )

    latest_df, requests = get_latest_data()
    log.info(f"Made {requests} requests while updating the data.")
    
    #latest_df.to_csv("data/fitbit.csv")
    
    commit_csv()
