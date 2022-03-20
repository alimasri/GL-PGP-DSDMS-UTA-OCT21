# Import necessary Libraries for slack, sql and dataframes
import requests
import sqlalchemy as db
from airflow.models import Variable
# --------------------------------------
# MySQL Database Constants
# --------------------------------------
database_ip = Variable.get('DATABASE_IP')
database_username = Variable.get("DATABASE_USERNAME")
database_password = Variable.get("DATABASE_PASSWORD")

# --------------------------------------
# Slack Messages
# --------------------------------------
# The slack webhook link will change as per the one generated for your app
slack_webhook = Variable.get("SLACK_WEBHOOK")

# Function to send slack message
def send_msg(text_string): requests.post(slack_webhook, json={'text': text_string}) 

# --------------------------------------
# SQL Query Functions
# --------------------------------------
# Create connection to MySQL database
engine = db.create_engine(f'mysql+pymysql://{database_username}:{database_password}@{database_ip}:3306/retail')

# Function to append a row into a SQL table
def append_row_sql(row, table):
    row.to_sql(table, engine, index=False, if_exists="append")