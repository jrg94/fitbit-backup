import base64
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
import requests
from git import Actor, Repo
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

log = logging.getLogger(__name__)


def automate_code_retrieval() -> str:
    """
    Grabs the initial code from the Fitbit website containing
    the correct scopes.

    :return: the code as a string
    """
    log.info("Starting the process of generating a new access token.")
    url = "https://www.fitbit.com/oauth2/authorize" \
        "?response_type=code" \
        f"&client_id={os.environ.get('FITBIT_CLIENT_ID')}" \
        "&redirect_uri=http%3A%2F%2F127.0.0.1%3A8080%2F" \
        "&scope=activity%20heartrate%20location%20nutrition%20profile%20settings%20sleep%20social%20weight%20oxygen_saturation%20respiratory_rate%20temperature" \
        "&expires_in=604800"

    # Get URL
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(url)

    # Complete login form
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//input[@type='email']")))
    username_input = driver.find_element(By.XPATH, "//input[@type='email']")
    password_input = driver.find_element(By.XPATH, "//input[@type='password']")
    submit = driver.find_element(
        By.XPATH, "//form[@id='loginForm']/div/button")
    username_input.send_keys(os.environ.get("FITBIT_USERNAME"))
    password_input.send_keys(os.environ.get("FITBIT_PASSWORD"))
    submit.click()

    # Get code
    WebDriverWait(driver, 10).until(EC.url_contains("127.0.0.1:8080"))
    code = driver.current_url.split("code=")[-1].split("#")[0]
    driver.quit()

    return code


def automate_token_retrieval(code: str):
    """
    Using the code from the Fitbit website, retrieves the
    correct set of tokens.
    """
    log.info("Using code to access new tokens.")
    data = {
        "clientId": os.environ.get("FITBIT_CLIENT_ID"),
        "grant_type": "authorization_code",
        "redirect_uri": "http://127.0.0.1:8080/",
        "code": code
    }
    basic_token = base64.b64encode(
        f"{os.environ.get('FITBIT_CLIENT_ID')}:{os.environ.get('FITBIT_CLIENT_SECRET')}".encode(
            "utf-8")
    ).decode("utf-8")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {basic_token}"
    }
    response = requests.post(data=data, headers=headers,
                             url="https://api.fitbit.com/oauth2/token")
    keys = response.json()
    dotenv.set_key(".env", "FITBIT_ACCESS_TOKEN", keys["access_token"])
    dotenv.set_key(".env", "FITBIT_REFRESH_TOKEN", keys["refresh_token"])
    dotenv.set_key(".env", "FITBIT_EXPIRES_AT", str(keys["expires_in"]))
    dotenv.load_dotenv(override=True)


def refresh_cb(token: dict) -> None:
    """
    Provides a mechanism for updating the Fitbit API tokens.

    :param token: a dictionary of token data
    """
    log.info("Refreshing Fitbit tokens.")
    if os.environ.get("FITBIT_ACCESS_TOKEN") != token["access_token"]:
        log.info("Updating FITBIT_ACCESS_TOKEN.")
        dotenv.set_key(".env", "FITBIT_ACCESS_TOKEN", token["access_token"])
    if os.environ.get("FITBIT_REFRESH_TOKEN") != token["refresh_token"]:
        log.info("Updating FITBIT_REFRESH_TOKEN.")
        dotenv.set_key(".env", "FITBIT_REFRESH_TOKEN", token["refresh_token"])
    dotenv.set_key(".env", "FITBIT_EXPIRES_AT", str(token["expires_at"]))
    dotenv.load_dotenv(override=True)


def commit_csv() -> None:
    """
    Commits the fitbit data to the personal data repo.
    """
    with tempfile.TemporaryDirectory() as dir:
        repo = Repo.clone_from(
            "https://github.com/jrg94/personal-data.git", dir
        )
        health_data_path = Path(dir) / "health"
        shutil.copyfile(
            "fitbit.csv",
            str(health_data_path / "fitbit.csv")
        )
        repo.index.add([str(health_data_path / "fitbit.csv")])
        author = Actor("GitHub Action", "action@github.com")
        commit = repo.index.commit(f"Updated fitbit data automatically", author=author)
        if not commit.stats.files:
            log.info("No changes to commit.")
        else:
            log.info(f"Committing changes: {commit.stats.files}")
            repo.remote(name="origin").push()
        repo.close()


def get_sleep_data(client, date: str, to_df: dict) -> None:
    """
    A helper function for retrieving sleep data.

    :param date: the date of sleep to pull
    :param to_df: the dictionary to add the data to
    """
    day_of_sleep: dict = client.sleep(date)
    log.info(f"Retrieve sleep data for {date}: {day_of_sleep}")
    day_of_sleep = day_of_sleep["summary"]
    day_of_sleep.pop("stages", None)
    if all(v > 0 for v in day_of_sleep.values()):
        to_df |= day_of_sleep


def get_steps_data(client, date: str, to_df: dict) -> None:
    """
    A helper function for retrieving steps data.

    :param date: the date of steps to pull
    :param to_df: the dictionary to add the data to
    """
    day_of_steps: dict = client.time_series(
        "activities/steps",
        base_date=date,
        period="1d"
    )
    log.info(f"Retrieve steps data for {date}: {day_of_steps}")
    day_of_steps = day_of_steps["activities-steps"][0]
    if day_of_steps["value"] != '0':
        to_df |= day_of_steps


def get_body_data(client, date: str, to_df: dict) -> None:
    """
    A helper function for retrieving body data.

    :param date: the date of body data to pull
    :param to_df: the dictionary to add the data to
    """
    day_of_body: dict = client.body(date)
    log.info(f"Retrieve body data for {date}: {day_of_body}")
    day_of_body = day_of_body["body"]
    if all(v > 0 for v in day_of_body.values()):
        to_df |= day_of_body


def get_heart_data(client, date: str, to_df: dict) -> None:
    """
    A helper function for retrieving heart data.

    :param date: the date of heart data to pull
    :param to_df: the dictionary to add the data to
    """
    day_of_heart = client.time_series(
        "activities/heart",
        base_date=date,
        period="1d"
    )
    log.info(f"Retrieve heart data for {date}: {day_of_heart}")
    day_of_heart = day_of_heart["activities-heart"][0]["value"].get(
        "restingHeartRate")
    if day_of_heart:
        to_df |= {"restingHeartRate": day_of_heart}


def get_row_of_data(client, date: str) -> tuple[pd.DataFrame, int]:
    """
    Grabs a day's worth of data from the Fitbit API.

    :param date: the date to pull data for
    :return: a dataframe of the data and the number of requests to generate a row
    """
    to_df = {}
    columns = {
        "dateTime": "Date",
        "value": "Steps",
        "bmi": "BMI",
        "fat": "Body Fat %",
        "weight": "Weight",
        "totalMinutesAsleep": "Total Sleep (minutes)",
        "totalSleepRecords": "Total Sleep Records",
        "totalTimeInBed": "Total Time in Bed (minutes)",
        "restingHeartRate": "Resting Heart Rate",
    }

    # Fitbit queries
    get_sleep_data(client, date, to_df)
    get_steps_data(client, date, to_df)
    get_body_data(client, date, to_df)
    get_heart_data(client, date, to_df)

    if not to_df:
        log.warning(f"No data for {date}")
        return None, 4
    else:
        df = pd.DataFrame([to_df])
        df.rename(columns=columns, inplace=True)
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        log.info(f"Collected a row of data:\n{df}")
        return df, 4


def get_latest_data(client):
    """
    Takes the existing data and updates it with the latest data.

    :return: a dataframe of the complete data set
    """
    requests = 0
    df = pd.read_csv(
        "https://raw.githubusercontent.com/jrg94/personal-data/main/health/fitbit.csv")
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)
    latest_date = df.index[-7]  # last 7 days to account for missing syncs
    date_range = pd.date_range(
        start=latest_date,
        end=datetime.today(),
        freq="D"
    )
    try:
        for date in date_range:
            row, curr = get_row_of_data(client, date)
            requests += curr
            if isinstance(row, pd.DataFrame):
                df = pd.concat([df, row])
    except fitbit.exceptions.HTTPTooManyRequests:
        log.warning(f"Reached rate limit on this run.")
    df = df[~df.index.duplicated(keep="last")]
    return df, requests


def main():
    # Load the .env file
    log.info("Loading .env file.")
    dotenv.load_dotenv()

    # Initiate the Fitbit API
    log.info("Initiating Fitbit API.")
    client = fitbit.Fitbit(
        os.environ.get("FITBIT_CLIENT_ID"),
        os.environ.get("FITBIT_CLIENT_SECRET"),
        access_token=os.environ.get("FITBIT_ACCESS_TOKEN"),
        refresh_token=os.environ.get("FITBIT_REFRESH_TOKEN"),
        refresh_cb=refresh_cb
    )

    # Collect data
    latest_df, requests = get_latest_data(client)
    log.info(f"Made {requests} requests while updating the data.")
    log.info(f"Finalized the latest data collection:\n{latest_df}")

    # Backup data
    latest_df.to_csv("fitbit.csv")
    commit_csv()
    os.remove("fitbit.csv")


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

    try:
        main()
    except Exception as e:
        log.exception(e)
        code = automate_code_retrieval()
        automate_token_retrieval(code)
        main()
