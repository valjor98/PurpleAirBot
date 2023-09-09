#      


import requests
import sqlite3
import pandas as pd
import time
import requests
import sqlite3
import pandas as pd
import time

# Mapping of API field names to SQLite column names
field_mapping = {
    "last_seen": "last_seen",
    "pm2.5_alt": "pm25_alt",
    "pm2.5_alt_a": "pm25_alt_a",
    "pm2.5_alt_b": "pm25_alt_b",
    "pm2.5": "pm25",
    "pm2.5_a": "pm25_a",
    "pm2.5_b": "pm25_b",
    "pm2.5_atm": "pm25_atm",
    "pm2.5_atm_a": "pm25_atm_a",
    "pm2.5_atm_b": "pm25_atm_b",
    "pm2.5_cf_1": "pm25_cf_1",
    "pm2.5_cf_1_a": "pm25_cf_1_a",
    "pm2.5_cf_1_b": "pm25_cf_1_b",
    "temperature_a": "temperature_a",
    "humidity_a": "humidity_a",
    "pressure_a": "pressure_a"
}

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
            # Renaming columns
            df.rename(columns=field_mapping, inplace=True)
            data_frames.append(df)
        else:
            print(f"Bad response for sensor {sensor_id}: {response.status_code}")

    return pd.concat(data_frames, ignore_index=True)

# Function to filter out sensor data that's older than a certain time
def AGEfilter(df, max_minutes):
    df['last_seen'] = pd.to_datetime(df['last_seen'], unit='s').dt.tz_localize('UTC')
    df['age'] = (pd.Timestamp.now(tz='UTC') - df['last_seen']).dt.total_seconds() / 60
    return df[df['age'] <= max_minutes]

# Initialize SQLite database
conn = sqlite3.connect("purpleair_data.db")
cursor = conn.cursor()

# Note: The column names here should match the SQLite-friendly names in the mapping
cursor.execute('''
CREATE TABLE IF NOT EXISTS SensorData (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    last_seen DATETIME,
    pm25_alt REAL,
    pm25_alt_a REAL,
    pm25_alt_b REAL,
    pm25 REAL,
    pm25_a REAL,
    pm25_b REAL,
    pm25_atm REAL,
    pm25_atm_a REAL,
    pm25_atm_b REAL,
    pm25_cf_1 REAL,
    pm25_cf_1_a REAL,
    pm25_cf_1_b REAL,
    temperature_a REAL,
    humidity_a REAL,
    pressure_a REAL,
    age REAL
)
''')
conn.commit()

# List of sensor IDs and fields
sensor_ids = ["37747"]  # TODO: change to sensors you want
fields = list(field_mapping.keys())  # This will take the keys from the field_mapping dictionary

# Pull data every 30 minutes
while True:
    sensor_data = getSensorsData(sensor_ids, fields)
    df = pd.DataFrame(sensor_data)
    filtered_data = AGEfilter(df, 20)
    filtered_data.to_sql("SensorData", conn, if_exists="append", index=False)
    print("Data inserted into database.")

    cursor.execute("SELECT * FROM SensorData ORDER BY id DESC LIMIT 5")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

    time.sleep(1800)  # Wait for 30 minutes
