import csv
import sqlite3
import pandas as pd
from flask import Flask, request, jsonify
import requests
from io import StringIO
import random
import string
import pytz
from datetime import datetime

app = Flask(__name__)
def get_db_connection():
    return sqlite3.connect('restaurant_data.db')

# Function to parse and store CSVs from Google Drive links
def parse_and_store_csvs():
    # Read store status CSV from Google Drive link
    conn = get_db_connection()
    store_status_data = pd.read_csv("C:\\Users\\gvsri\\Downloads\\store status.csv")  # Replace 'path_to_store_status_csv.csv' with the actual file path

    # Store the store status data into the database
    store_status_data.to_sql('store_status', conn, if_exists='replace', index=False)

    # Read business hours CSV from local file
    business_hours_data = pd.read_csv("C:\\Users\\gvsri\\Downloads\\Menu hours.csv")  # Replace 'path_to_business_hours_csv.csv' with the actual file path

    # Store the business hours data into the database
    business_hours_data.to_sql('business_hours', conn, if_exists='replace', index=False)

    # Read store timezone CSV from local file
    store_timezone_data = pd.read_csv("C:\\Users\\gvsri\\Downloads\\bq-results-20230125-202210-1674678181880.csv")  # Replace 'path_to_store_timezone_csv.csv' with the actual file path

    # Assuming Chicago timezone if missing
    store_timezone_data.fillna('America/Chicago', inplace=True)

    # Store the store timezone data into the database
    store_timezone_data.to_sql('store_timezone', conn, if_exists='replace', index=False)
    conn.close()

# Calculate Uptime and Downtime
# Calculate Uptime and Downtime
# Calculate Uptime and Downtime
def calculate_uptime_downtime():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM store_status")
    store_statuses = cursor.fetchall()
    
    uptime_downtime = {}
    for store_status in store_statuses:
        store_id, timestamp_utc, status = store_status
        
        # Check if the timestamp is a valid datetime string
        try:
            utc_time = datetime.strptime(timestamp_utc, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            # Skip processing this entry if the timestamp is not valid
            continue
        
        # Retrieve store's business hours and timezone
        cursor.execute("SELECT * FROM business_hours WHERE store_id = ?", (store_id,))
        business_hours = cursor.fetchall()
        print(business_hours)
        timezone_row = cursor.execute("SELECT timezone_str FROM store_timezone WHERE store_id = ?", (store_id,)).fetchone()
        if timezone_row:
            timezone_str = timezone_row[0]
        else:
            timezone_str = 'America/Chicago'  # Default timezone if not found
        
        for business_hour in business_hours:
            # Convert UTC timestamp to store's local time
            store_id = business_hour[0]
            day_of_week = business_hour[1]
            start_time_local, end_time_local = business_hour[2], business_hour[3]
            start_time_local
            end_time_local = business_hours[1], business_hours[2]
            utc_time = datetime.strptime(timestamp_utc, '%Y-%m-%d %H:%M:%S')
            utc_time = pytz.utc.localize(utc_time)
            local_timezone = pytz.timezone(timezone_str)
            local_time = utc_time.astimezone(local_timezone)
            
            start_time_local = datetime.strptime(start_time_local, '%H:%M:%S').time()
            end_time_local = datetime.strptime(end_time_local, '%H:%M:%S').time()
            
            # Check if the status was recorded within business hours
            if local_time.time() >= start_time_local and local_time.time() <= end_time_local:
                if store_id not in uptime_downtime:
                    uptime_downtime[store_id] = {'uptime': 0, 'downtime': 0,'day_of_week':day_of_week}
                
                if status == 'active':
                    uptime_downtime[store_id]['uptime'] += 1
                else:
                    uptime_downtime[store_id]['downtime'] += 1
    
    conn.close()
    return uptime_downtime


# Generate Report
def generate_report():
    uptime_downtime = calculate_uptime_downtime()
    report_data = []
    for store_id, times in uptime_downtime.items():
        uptime_last_hour = times['uptime']
        downtime_last_hour = times['downtime']
        uptime_last_day = uptime_last_hour * 24
        downtime_last_day = downtime_last_hour * 24
        uptime_last_week = uptime_last_day * 7
        downtime_last_week = downtime_last_day * 7
        report_data.append((store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week))
    
    report_df = pd.DataFrame(report_data, columns=['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 'downtime_last_hour', 'downtime_last_day', 'downtime_last_week'])
    
    # Print the generated report data for debugging
    print("Generated report data:")
    print(report_df)
    
    return report_df


# Trigger Report Generation
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    conn = get_db_connection()
    parse_and_store_csvs()
    report_df = generate_report()
    report_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))  # Generate random report ID
    report_df.to_sql(f'report_{report_id}', conn, if_exists='replace', index=False)
    conn.close()
    return jsonify({'report_id': report_id})

# Get Report
@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')
    if not report_id:
        return jsonify({'error': 'Report ID not provided'})

    conn = sqlite3.connect('restaurant_data.db')
    cursor = conn.cursor()

    # Check if the report table exists in the database
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='report_{report_id}'")
    if not cursor.fetchone():
        conn.close()
        print(f"Report table report_{report_id} not found.")
        return jsonify({'error': 'Report not found'})

    try:
        # Attempt to read the report data from the database
        cursor.execute(f"SELECT * FROM report_{report_id}")
        report_data = cursor.fetchall()
        conn.close()

        if not report_data:
            print("Report data not found.")
            return jsonify({'error': 'Report data not found'})

        # Convert fetched data into CSV format
        csv_data = ','.join([','.join(map(str, row)) for row in report_data])
        print("CSV data generated successfully.")
        return csv_data, 200, {'Content-Type': 'text/csv'}
    except Exception as e:
        conn.close()
        print("Error occurred:", e)
        return jsonify({'error': str(e)})


if __name__ == '__main__':
    app.run(debug=True)
