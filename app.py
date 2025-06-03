from flask import Flask, request, jsonify, send_file
import mysql.connector
import pandas as pd
import requests
import config

app = Flask(__name__)

API_KEY = config.OPENWEATHERMAP_API_KEY
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

def get_weather_data(location):
    params = {"q" : location, "appid" : API_KEY, "units" : "metric"}
    response = requests.get(BASE_URL, params = params)
    return response.json()

def db_connection():
    return mysql.connector.connect(
        host = config.MYSQL_HOST,
        user = config.MYSQL_USER,
        password = config.MYSQL_PASSWORD,
        database = config.MYSQL_DATABASE
    )

@app.route('/weather/create', methods = ['POST'])
def create_weather_entry():
    data = request.json
    location = data.get("location")
    date = data.get("date")
    if not location or not date:
        return jsonify({"error" : "Location and Date are required!"}), 400
    weather_data = get_weather_data(location)
    if "main" not in weather_data:
        return jsonify({"error" : "Invalid Location!"}), 400
    temperature = weather_data["main"]["temp"]
    humidity = weather_data["main"]["humidity"]
    condition = weather_data["weather"][0]["description"]
    
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO weather_data (location, date, temperature, humidity, weather_condition) VALUES (%s, %s, %s, %s, %s)",
        (location, date, temperature, humidity, condition)
    )
    conn.commit()
    conn.close()
    
    return jsonify({"message" : "Weather entry created successfully", "temperature" : temperature})

@app.route('/weather/read', methods = ['GET'])
def read_weather_entry():
    location = request.args.get("location")
    date = request.args.get("date")
    if not location or not date:
        return jsonify({"error" : "Location and Date are required!"}), 400
    conn = db_connection()
    cursor = conn.cursor(dictionary = True)
    cursor.execute("SELECT * FROM weather_data WHERE location = %s AND date = %s", (location, date))
    result = cursor.fetchone()
    conn.close()
    if not result:
        return jsonify({"error" : "No data found!"}), 404
    return jsonify(result)

@app.route('/weather/update', methods = ['PUT'])
def update_weather_entry():
    data = request.json
    location = data.get("location")
    date = data.get("date")
    temperature = data.get("temperature")
    humidity = data.get("humidity")
    condition = data.get("weather_condition")
    if not location or not date:
        return jsonify({"error" : "Location and Date are required!"}), 400
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE weather_data SET temperature = %s, humidity = %s, weather_condition = %s WHERE location = %s AND date = %s",
        (temperature, humidity, condition, location, date)
    )
    conn.commit()
    conn.close()
    return jsonify({"message" : "Weather entry updated successfully!"})

@app.route('/weather/delete', methods = ['DELETE'])
def delete_weather_entry():
    location = request.args.get("location")
    date = request.args.get("date")
    if not location or not date:
        return jsonify({"error" : "Location and Date are required!"}), 400
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM weather_data WHERE location = %s AND date = %s", (location, date))
    conn.commit()
    conn.close()
    return jsonify({"message" : "Weather entry deleted successfully!"})

@app.route('/weather/export', methods=['GET'])
def export_weather_data():
    conn = db_connection()
    cursor = conn.cursor(dictionary=True)
    query = "SELECT * FROM weather_data"
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    if not result:
        return {"error": "No data found"}, 404
    df = pd.DataFrame(result)
    csv_filename = "weather_data.csv"
    df.to_csv(csv_filename, index=False)
    file_response = send_file(csv_filename, as_attachment=True)
    return file_response

if __name__ == '__main__':
    app.run(debug = True)