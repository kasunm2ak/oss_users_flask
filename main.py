from flask import Flask
import pymysql
import pandas as pd
import os
import hashlib
import time
from sqlalchemy import create_engine, text

app = Flask(__name__)

# MySQL connection details
db_host = "172.22.110.35"
db_user = "root"
db_password = "root123"
db_name = "OSS_USERS"
db_port = 5600

# Excel file path
excel_file_path = "C:/Users/kasunma/PycharmProjects/OSS_USER/data/Export User and Department Data_RAN_EMS.xls"

def create_table():
    # Connect to MySQL
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    # Read Excel into a DataFrame
    df = pd.read_excel(excel_file_path, skiprows=1)
    column_mapping = {
        'User Name': 'username',
        'Full Name': 'fullname',
        'User Description': 'userdescri',
        'Phone Number': 'phonenum',
        'Email Address': 'email',
        'User Department': 'userdep',
        'Password': 'password',
        'Encrypt Version': 'encryptver',
        'Role Name': 'role',
        'Role Set Name': 'roleset',
        'Logs Can Be Viewed by the Role': 'logscanbyrole',
        'Logs Can Be Viewed by the Role Set': 'logscanbyroleset',
        'Disabled': 'disable',
        'The Maximum Password Age(days)': 'maxpassage',
        'The Minimum Password Age(days)': 'minpassage',
        'Account Validity Time(days)': 'accountvaliditytime',
        'Must Change Password Before Next Login': 'mustchangepasswd',
        'Cannot Change Password': 'cannotchgpasswd',
        'Creator': 'creator',
        'In Black List': 'inbalcklist',
        'Reason In Black List': 'reasonforblacklist',
        'Concurrent Logins': 'concurrentlogins',
        'Login Type': 'logintype',
        'Working Time': 'workingtime',
        'Holidays out of Working Time': 'holidaysout',
        'IP Range': 'iprange',
        'GUI MAC Binding': 'guimacbind',
        'Minimum Password Length (characters)': 'minpasswdlen',
        'Cannot Be Last Used Password(s)': 'cannotlastpasswd',
        'Lock at Password Error(times)': 'lockatpasswderror',
        'Maximum Password Length (characters)': 'maxpasswdlen',
        'Logout Idle Time (minutes)': 'logoutideltime',
        'Connect Type': 'connecttype',
        'Account expiration time': 'accountexpertime',
        'Account creation time': 'accountcreatetime',
    }
    df.rename(columns=column_mapping, inplace=True)
    # Get column names and types
    columns = ", ".join([f"{col} VARCHAR(255)" for col in df.columns])

    # Create table if not exists
    create_table_sql = f"CREATE TABLE IF NOT EXISTS RAN_PORTAL ({columns})"
    with engine.connect() as connection:
        connection.execute(text(create_table_sql))

    # Insert data into MySQL table
    df.to_sql('RAN_PORTAL', con=engine, if_exists='replace', index=True)

    # Close the MySQL connection
    #connection.close()

def get_file_checksum(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def has_file_changed(file_path, last_checksum):
    return get_file_checksum(file_path) != last_checksum

@app.route('/')
def update_table():
    last_checksum = get_file_checksum(excel_file_path)
    create_table()

    while True:
        if has_file_changed(excel_file_path, last_checksum):
            create_table()
            last_checksum = get_file_checksum(excel_file_path)
        time.sleep(60)  # Sleep for 60 seconds before checking again

if __name__ == '__main__':
    app.run(debug=True)
    #create_table()

