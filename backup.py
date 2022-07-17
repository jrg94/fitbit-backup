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
        
        
def get_fitbit_data(time_series: str, period: str, key: str, freq: str = pd.offsets.YearEnd()) -> tuple[list[dict], int]:
    """
    Returns all data for a given time series.
    
    :param time_series: the time series to retrieve (e.g., activities/steps)
    :param period: the period to retrieve (e.g., 1d, 7d, 30d)
    :param key: the key to use for retrieving the sample from the API payload (e.g., activities-steps)
    :param freq: the frequency for filtering dates as defined in pd.date_range
    :return: a tuple of the data and the number of requests made for tracking purposes
    """
    
    # Define dates to pull data for
    log.info("Defining dates to pull data for.")
    first_day = datetime(2015, 7, 26)
    last_day = datetime.today().replace(year=datetime.today().year + 1)
    
    # Pull data
    log.info("Pulling data.")
    requests = 0
    date_range = pd.date_range(first_day, last_day, freq=freq)
    data_set = []
    for interval in date_range:
        year_of_steps = client.time_series(time_series, period=period, base_date=interval)
        log.info(f"{interval}: Retrieved {year_of_steps}.")
        data_set.extend(year_of_steps[key])
        requests += 1
        
    log.info(f"Collected raw data:\n{data_set}")
    return data_set, requests


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

    # Pull data
    requests = 0
    steps_raw, steps_requests = get_fitbit_data("activities/steps", "1y", "activities-steps")
    requests += steps_requests
    steps_data = pd.DataFrame(steps_raw)
    log.info("Processing data.")
    steps_data.rename(columns={"dateTime": "Date", "value": "Steps"}, inplace=True)
    steps_data["Steps"] = steps_data["Steps"].astype(int)
    steps_data = steps_data[steps_data["Steps"] > 0]
    steps_data.set_index("Date", inplace=True)
    
    weight_raw, weight_requests = get_fitbit_data("body/log/weight", "1m", "weight", pd.offsets.MonthEnd())
    requests += weight_requests
    log.info(f"Completed {requests} requests out of our 150 limit.")
    weight_data = pd.DataFrame(weight_raw)
    weight_data.rename(
        columns={
            "date": "Date", 
            "weight": "Weight", 
            "bmi": "BMI", 
            "fat": "Body Fat %", 
            "source": "Weight Source",
            "time": "Weight Time"
        }, 
        inplace=True
    )
    weight_data.drop("logId", axis=1, inplace=True)
    weight_data["Weight"] = weight_data["Weight"].astype(float)
    weight_data.set_index("Date", inplace=True)
    
    data_set = steps_data.join(weight_data, how="outer")

    # Store data
    log.info(f"Finalized data before pushing to CSV:\n{data_set}")
    data_set.to_csv("data/fitbit.csv")

    # Commit data to git
    commit_csv()
