import json
import sqlite3
import pandas as pd
from flask import Flask, request

app = Flask(__name__)

@app.route('/')
def hello():
	query = request.args['q']
	data = json.loads(query)
	record = {}
	fields = ["TIME", "USER", "PLATFORM", "TYPE", "SUBTYPE", "STATUS", "CURRENT", "EVENT", "EXTRA"]
	for i, value in enumerate(fields):
		record[value] = data[i]
	conn = sqlite3.connect("database.db")
	df = pd.DataFrame([record])
	df.to_sql('records', conn, if_exists='append', index=False)
	res = pd.read_sql('SELECT COUNT(*) as size FROM records', conn)
	result = {"status": "OK", "value": int(res.loc[0]['size'])}
	return json.dumps(result)

if __name__ == '__main__':
	app.run(host='0.0.0.0')

