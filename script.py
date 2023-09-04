import requests
import sqlite3
import pandas as pd
import time



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
    print("data:", conn)
    filtered_data.to_sql("SensorData", conn, if_exists="append", index=False)
    print("Data inserted into database.")

    # Query to fetch the most recent 5 records
    cursor.execute("SELECT * FROM SensorData ORDER BY id DESC LIMIT 5")
    rows = cursor.fetchall()

    # Print the fetched records
    for row in rows:
        print(row)
    time.sleep(1800)  # Wait for 30 minutes
