import json
import sqlite3
import pandas as pd
from flask import Flask, request
from sqlalchemy import create_engine

eng = 'mysql'
db = 'overhead'
opersys = 'linux'
table = 'records'
app = Flask(__name__)

def get_conn():
	if eng == 'sqlite':
		return sqlite3.connect("database.db")
	else:
		unix_sock = '/Applications/MAMP/tmp/mysql/mysql.sock' if opersys == 'mac' else '/var/run/mysqld/mysqld.sock'
		con_str = f'mysql+mysqlconnector://root:root@/{db}?unix_socket={unix_sock}'
		return create_engine(con_str)

@app.route('/')
def hello():
	if 'q' in request.args:
		query = request.args['q']
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
		if num_rows == 1 and db == 'mysql':
			conn.execute(f'ALTER TABLE {table} ADD id int NOT NULL AUTO_INCREMENT primary key FIRST;')
		result = {"status": "OK", "value": num_rows}
		return json.dumps(result)
	else:
		return json.dumps({"status": "ERROR", "value":"No parameters"})

if __name__ == '__main__':
	app.run(host='0.0.0.0')

