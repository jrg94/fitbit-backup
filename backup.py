from datetime import datetime
import fitbit
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

CLIENT_ID = os.environ.get("FITBIT_CLIENT_ID")
CLIENT_SECRET = os.environ.get("FITBIT_CLIENT_SECRET")
ACCESS_TOKEN = os.environ.get("FITBIT_ACCESS_TOKEN")
REFRESH_TOKEN = os.environ.get("FITBIT_REFRESH_TOKEN")

client = fitbit.Fitbit(CLIENT_ID, CLIENT_SECRET, access_token=ACCESS_TOKEN, refresh_token=REFRESH_TOKEN)

first_day = datetime(2015, 7, 26)
last_day = datetime.today().replace(year=datetime.today().year + 1)

date_range = pd.date_range(first_day, last_day, freq=pd.offsets.YearEnd())
steps_raw = []
for year in date_range:
    print(f"{int(year.strftime('%Y'))}{'='*20}")
    year_of_steps = client.time_series("activities/steps", period="1y", base_date=year)
    steps_raw.extend(year_of_steps["activities-steps"])
steps = pd.DataFrame(steps_raw)
steps["value"] = steps["value"].astype(int)
steps = steps[steps["value"] > 0]
print(steps)
