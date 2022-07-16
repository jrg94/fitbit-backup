import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime

import dotenv
import fitbit
import pandas as pd

log_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "logs", "fitbit.log")
logging.basicConfig(
    handlers=[RotatingFileHandler(log_path, backupCount=10, maxBytes=1000000)],
    level=logging.DEBUG,
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
)
log = logging.getLogger(__name__)


def refresh_cb(token: dict):
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

# Define dates to pull data for
log.info("Defining dates to pull data for.")
first_day = datetime(2015, 7, 26)
last_day = datetime.today().replace(year=datetime.today().year + 1)

# Pull data
log.info("Pulling data.")
date_range = pd.date_range(first_day, last_day, freq=pd.offsets.YearEnd())
steps_raw = []
for year in date_range: 
    year_of_steps = client.time_series("activities/steps", period="1y", base_date=year)
    steps_raw.extend(year_of_steps["activities-steps"])
steps = pd.DataFrame(steps_raw)

# Process data
log.info("Processing data.")
steps.rename(columns={"dateTime": "Date", "value": "Steps"}, inplace=True)
steps["Steps"] = steps["Steps"].astype(int)
steps = steps[steps["Steps"] > 0]
steps.set_index("Date", inplace=True)

steps.to_csv("data/fitbit.csv")
print(steps)
