D:
cd D:\Projects\fitbit-backup
py -m venv venv
call venv\Scripts\activate
pip install -r requirements.txt
py backup.py
