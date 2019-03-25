from flask import Flask, request, jsonify, json
from cassandra.cluster import Cluster
import requests

cluster = Cluster(['cassandra'])
session = cluster.connect()

app = Flask(__name__)

cities = [
	{'id': 1, 'name' : 'London', 'original': 'London'},
	{'id': 2, 'name' : 'Islamabad', 'original' : 'Islamabad'},
	{'id': 3, 'name' : 'Tehran', 'original' : 'Tehran'},
	{'id': 4, 'name' : 'Madrid', 'original' : 'Madrid'},
	{'id': 5, 'name' : 'Tokyo', 'original' : 'Tokyo'},
	{'id': 6, 'name' : 'Mexico City', 'original' : 'Mexico City'},
]

API_KEY = '5cbb13c2517ad300c52fa1806fd7d94f'

base_url = 'http://api.openweathermap.org/data/2.5/weather?q={city}&units={units}&APPID={API_KEY}'

@app.route('/')
def hello():
	name = request.args.get('name', 'World')
	return ('<h1>Hello, {}!<h1>'.format(name))


@app.route('/weather/<string:city>', methods=['GET'])
def get_weather(city):
	data = session.execute("""SELECT * FROM weather.city WHERE name = '{}' ALLOW FILTERING""".format(city))
	
	weather_url = base_url.format(city = city, units = 'metric', API_KEY = API_KEY)

	resp = requests.get(weather_url)

	if resp.ok:
		# print(resp.json())

		print(data)
		res = resp.json()
		weather = {
			'id': data.id,
			'city': city,
			'temperature': res['main']['temp'],
			'description': res['weather'][0]['description'],
			'icon' : res['weather'][0]['icon']
		}

		return jsonify(weather), resp.status_code
	else:
		return resp.reason

@app.route('/weather/<int:id>', methods=['GET'])
def get_weather_by_id(id):
	
	#data = [x for x in cities if x['id'] == id]
	data = session.execute("""SELECT * FROM weather.city WHERE id = '{}' ALLOW FILTERING""".format(id))
	print(data)
	weather_url = base_url.format(city = data.original, units = 'metric', API_KEY = API_KEY)

	resp = requests.get(weather_url)

	if resp.ok:
		# print(resp.json())
		print(data)
		res = resp.json()
		weather = {
			'id': id,
			'city': data.name,
			'original': data.original,
			'temperature': res['main']['temp'],
			'description': res['weather'][0]['description'],
			'icon' : res['weather'][0]['icon']
		}
		print(weather)

		return jsonify(weather), resp.status_code
	else:
		return resp.reason

@app.route('/weather', methods=['GET'])
def get_weather_for_all():
	weather_data = []
	
	cities = session.execute("SELECT * FROM weather.city")

	print(cities)
	for city in cities:
		print(city)
		weather_url = base_url.format(city = city.original, units = 'metric', API_KEY = API_KEY)
		print('before call')
		resp = requests.get(weather_url).json()

		if resp['cod'] == '404':
			continue
		print('before weather')
		weather = {
			'id': city.id,
			'city': city.name,
			'original': city.original,
			'temperature': resp['main']['temp'],
			'description': resp['weather'][0]['description'],
			'icon' : resp['weather'][0]['icon']
		}
		print('after weather')

		weather_data.append(weather)


	return jsonify(weather_data)

@app.route('/weather', methods = ['POST'])
def create_city():

	if not request.form or not 'city' in request.form:
		return jsonify({'Error': 'The new record needs to have a city name'}), 400
	
	weather_url = base_url.format(city = request.form['city'], units = 'metric', API_KEY = API_KEY)
	resp = requests.post(weather_url, data = {'city': request.form['city']} ).json()
	last_id = cities[-1]['id'];
	last_id += 1
	print(last_id)
	session.execute("""INSERT INTO weather.city(ID,Name,Original,Temperature,Description,Icon) VALUES({id},'{name}','{original}',{temperature},'{description}','{icon}')""".format(id=last_id, name=request.form['city'], original=request.form['city'], temperature=resp['main']['temp'], description=resp['weather'][0]['description'],icon=resp['weather'][0]['icon']))
	#cities.append({'id': last_id, 'name':request.form['city'], 'original':request.form['city']})
	
	return jsonify({'message': 'created new city with weather'})

@app.route('/weather/<int:id>', methods = ['PUT'])
def update_city(id):

	if not request.json or not 'city' in request.json:
		return jsonify({'Error': 'Record does not exist'}), 404

	#session.execute("""IUPDATE weather.city SET Description='{description}' WHERE name='{name}')""".format(name=city, description=city))
	for x in cities:
		if x['id'] == id:

			x['name'] = request.json['city']

			break
	get_weather_for_all()

	return jsonify({'message':'updated: /weather/{}'.format(id)})


@app.route('/weather/<string:city>', methods = ['DELETE'])
def delete_city(city):
	if not city:
		return jsonify({'Error': 'The city name is needed to delete'})
	
	#session.execute("""DELETE FROM weather.city WHERE name='{}'""".format(city))

	for x in cities:
		if x['name'] == city:
			cities.remove(x)
			break

	return jsonify({'message': 'deleted: /weather/{}'.format(city)}) 

@app.route('/weather/<int:id>', methods = ['DELETE'])
def delete_city_by_id(id):
	if not id:
		return jsonify({'Error': 'The id is needed to delete'})
	
	#session.execute("""DELETE FROM weather.city WHERE name='{}'""".format(city))

	for x in cities:
		if x['id'] == id:
			print(cities)
			cities.remove(x)
			print(cities)
			break
	
	return jsonify({'message': 'deleted: /weather/{}'.format(id)}) 


@app.route('/pokemon/<name>')
def profile(name):
	rows = session.execute("""SELECT * FROM pokemon.stats WHERE name = '{}'""".format(name))
	for pokemon in rows:
		return('<h1>{} has {} attack!</h1>'.format(name, pokemon.attack))

	return('<h1>That Pokemon does not exist!</h1>')

if __name__ == '__main__':
	app.run(port=8080)