# Fitbit Backup
An experimental tool for backing up Fitbit data as a CSV. 

If you'd like to make use of it yourself, clone this repo and include a `.env`
in the root folder in the following format:

```env
FITBIT_USERNAME="SOME_FITBIT_USERNAME"
FITBIT_PASSWORD="SOME_PASSWORD"
FITBIT_CLIENT_ID='SOME_CLIENT_ID'
FITBIT_CLIENT_SECRET='SOME_FITBIT_CLIENT_SECRET'
FITBIT_ACCESS_TOKEN='SOME_FITBIT_CLIENT_ACCESS_CODE'
FITBIT_REFRESH_TOKEN='SOME_REFRESH_TOKEN'
FITBIT_EXPIRES_AT='SOME_NUMBER'
```

To keep things simple, you can actually just fill in the first four
items (i.e., username, password, client ID, and client secret). The
rest will automatically populate on the first execution.

Also, you will likely need to tweak the code because it currently
commits to my repository (which will cause the program to crash).
Feel free to swap out the link in the code with yours. Future work
may be done to include the github repo in this dotenv file.

Finally, don't forget to install everything in the requirements.txt
file. 
