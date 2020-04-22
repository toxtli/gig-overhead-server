import os
import json
import time
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, request
from sqlalchemy import create_engine

load_dotenv()
eng = 'mysql'
db = 'overhead_all'
table = 'records'
data_dir = 'data'
db_cred = os.environ['DB_CRED']
opersys = os.environ['OPERSYS']
app = Flask(__name__)

def get_conn():
	if eng == 'sqlite':
		return sqlite3.connect("database.db")
	else:
		unix_sock = '/Applications/MAMP/tmp/mysql/mysql.sock' if opersys == 'mac' else '/var/run/mysqld/mysqld.sock'
		con_str = f'mysql+mysqlconnector://{db_cred}@/{db}?unix_socket={unix_sock}'
		return create_engine(con_str)

@app.route('/', methods=['GET', 'POST'])
def hello():
	result = {"status": "ERROR", "value": "The parameters were not set"}
	if 'a' in request.values and 'q' in request.values:
		if request.values['a'] == 'store':
			query = request.values['q']
			data = json.loads(query)
			record = {}
			fields = ["TIME", "USER", "PLATFORM", "TYPE", "SUBTYPE", "STATUS", "CURRENT", "EVENT", "EXTRA"]
			for i, value in enumerate(fields):
				record[value] = data[i]
			df = pd.DataFrame([record])
			conn = get_conn()
			df.to_sql(table, conn, if_exists='append', index=False)
			res = pd.read_sql('SELECT COUNT(*) as size FROM records', conn)
			num_rows = int(res.loc[0]['size'])
			if num_rows == 1 and eng == 'mysql':
				conn.execute(f'ALTER TABLE {table} ADD id int NOT NULL AUTO_INCREMENT primary key FIRST;')
			return json.dumps({"status": "OK", "value": num_rows})
		elif request.values['a'] == 'local':
			query = request.values['q']
			data = json.loads(query)
			user_id = data['user_id']
			timestamp = int(time.time())
			dirname = f"{data_dir}/{user_id}"
			if not os.path.exists(dirname):
				os.mkdir(dirname)
			filename = f"{dirname}/{timestamp}.json"
			with open(filename, 'w') as f:
				f.write(query)
			return json.dumps({"status": "OK", "value": filename})
		else:
			return json.dumps({"status": "OK", "value": "Action not supported"})
	return json.dumps(result)

if __name__ == '__main__':
	app.run(host='0.0.0.0')

