import requests
import sqlite3
import pandas as pd
import time


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
