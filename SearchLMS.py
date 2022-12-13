# !/Users/matbook/Projects/SearchLMS/.venv/bin/
import oracledb
import os
import pandas as pd
import numpy as np
import openpyxl
from dotenv import load_dotenv
from pandas import DataFrame
load_dotenv()

user = os.environ.get('SQL_USER')
password = os.environ.get('SQL_PASSWORD')


with oracledb.connect(user=user, password=password,dsn="prd",config_dir=".venv/") as connection:
	with connection.cursor() as cursor:
		with open('query.sql') as f:
			full_sql = f.read()

			df = pd.DataFrame(cursor.execute(full_sql), columns=['Product','Batch','Methods','Customer','QC Sample In','Analytical Done','Micro Sample In','CoA Issued'])
df.to_excel("newfile.xlsx")