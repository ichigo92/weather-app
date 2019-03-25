from flask import Flask, request, jsonify, json
from cassandra.cluster import Cluster
import requests

cluster = Cluster(['cassandra'])
session = cluster.connect()

app = Flask(__name__)

@app.route('/')
def hello():
	name = request.args.get('name', 'World')
	return ('<h1>Hello, {}!</h1>'.format(name))

@app.route('/weather', methods=['GET'])
def get_all_weather():
	print('inside get_all_weather')
	rows = session.execute("SELECT * FROM weather.city")
		for row in rows:
			print row.id row.name row.original row.temperature row.description

@app.route('/weather/<int:id>', methods=['GET'])
def get_weather_by_id(id):
	print('inside get_weather_by_id')
	rows = session.execute("SELECT * FROM weather.city WHERE id=%s")
	for row in rows:
		print row.id row.name row.original row.temperature row.description

@app.route('/weather', methods=['POST'])
def create_city():
	print('inside create_city')
	rows = session.execute("""INSERT INTO weather.city (id, name, original, temperature, description, icon) VALUES (%(id)s, %(name)s, %(original)s, %(temperature)s, %(description)s, %(icon)s)""",
    {'id': 7, 'name': 'Vancouver', 'original': 'Vancouver', 'temperature': 14.5, 'description': 'black clouds', 'icon': '02n'})

    print(rows)
    print(rows['applied'])

@app.route('/weather/<int:id>', methods = ['PUT'])
def update_city(id):
	print('inside update_city')
	print(request.json['city'])
	rows = session.execute("""UPDATE weather.city SET name=%(name)s WHERE id=%(id)s""", {'name': request.json['city'], 'id': id})
	print(rows)
	print(rows[0])
	print(rows['applied'])



@app.route('/weather/<int:id>', methods = ['DELETE'])
def delete_city(id):
	print('inside delete_city')

	rows = session.execute("""DELETE FROM weather.city WHERE id=%(id)s""", {'id' = id})
	print(rows)
	print(rows[0])
	print(rows['applied'])


