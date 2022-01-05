import os
import json
import time

import sqlite3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from flask_cors import CORS
from flask import Flask, request
from flask_socketio import SocketIO, send, emit

load_dotenv()
eng = 'sqlite'
db = 'toloka_all'
table = 'records'
data_dir = 'data'
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")
con = None

def get_conn():
	global con
	if eng == 'sqlite':
		return sqlite3.connect("database.db")
	else:
		db_cred = os.environ['DB_CRED']
		opersys = os.environ['OPERSYS']
		unix_sock = '/Applications/MAMP/tmp/mysql/mysql.sock' if opersys == 'mac' else '/var/run/mysqld/mysqld.sock'
		con_str = f'mysql+mysqlconnector://{db_cred}@/{db}?unix_socket={unix_sock}'
		if con is None:
			con = create_engine(con_str)
		return con

@app.route('/api', methods=['GET', 'POST'])
def hello():
	result = {"status": "ERROR", "value": "The parameters were not set"}
	use_count = False
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
			if use_count:
				res = pd.read_sql('SELECT COUNT(*) as size FROM records', conn)
				num_rows = int(res.loc[0]['size'])
			else:
				num_rows = 0
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

@socketio.on('myevent')
def handle_myevent(data):
	# print('myevent')
	df = pd.DataFrame(data).applymap(str)
	df.to_sql('stream', get_conn(), if_exists='append', index=False)
	emit('myresponse', data, broadcast=True)

@socketio.on('connect')
def test_connect(auth):
	# print('Client connected')
	emit('myresponse', {'data': 'Connected'})

@socketio.on('disconnect')
def test_disconnect():
    # print('Client disconnected')
    pass

if __name__ == '__main__':
	# app.run(host='0.0.0.0', ssl_context='adhoc')
	# app.run(host='0.0.0.0')
	socketio.run(app, host='0.0.0.0')
