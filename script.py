import requests
import sqlite3
import pandas as pd
import time

# Function to get sensor data from PurpleAir API
def getSensorsData(sensor_ids, fields):
    base_url = "https://api.purpleair.com/v1/sensors/"
    api_key = "" # TODO change to be en env variable  
    data_frames = []

    for sensor_id in sensor_ids:
        url = f"{base_url}{sensor_id}?api_key={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            sensor_data = response.json()["sensor"]
            selected_data = {field: sensor_data.get(field) for field in fields}
            df = pd.DataFrame([selected_data])
            data_frames.append(df)
        else:
            print(f"Bad response for sensor {sensor_id}: {response.status_code}")

    return pd.concat(data_frames, ignore_index=True)

# Function to filter out sensor data that's older than a certain time
def AGEfilter(df, max_minutes):
    # Convert Unix timestamps to timezone-aware datetime objects
    df['last_seen'] = pd.to_datetime(df['last_seen'], unit='s').dt.tz_localize('UTC')

    # Compute the age in minutes
    df['age'] = (pd.Timestamp.now(tz='UTC') - df['last_seen']).dt.total_seconds() / 60

    # Filter based on age
    return df[df['age'] <= max_minutes]

# Initialize SQLite database
conn = sqlite3.connect("purpleair_data.db")
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS SensorData (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    last_seen DATETIME,
    pm2_5_60minute_a REAL,
    pm2_5_60minute_b REAL,
    temperature_a REAL,
    humidity_a REAL,
    pressure_a REAL,
    age REAL
)
''')
conn.commit()

# List of sensor IDs and fields
sensor_ids = ["36721"]  # TODO change to sensors we want
fields = ["last_seen", "pm2_5_60minute_a", "pm2_5_60minute_b", "temperature_a", "humidity_a", "pressure_a"]

# Pull data every 30 minutes
while True:
    # Fetch data from PurpleAir API
    sensor_data = getSensorsData(sensor_ids, fields)
    # Filter out old data
    df = pd.DataFrame(sensor_data)
    filtered_data = AGEfilter(df, 20)
    # Insert data into SQLite database
    filtered_data.to_sql("SensorData", conn, if_exists="append", index=False)
    print("Data inserted into database.")

    # Query to fetch the most recent 5 records
    cursor.execute("SELECT * FROM SensorData ORDER BY id DESC LIMIT 5")
    rows = cursor.fetchall()

    # print the fetched data every time there's an insert
    for row in rows:
        print(row)
    time.sleep(1800)  # Wait for 30 minutes
