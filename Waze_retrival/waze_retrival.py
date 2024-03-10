import os
import datetime
import csv
import uuid
import schedule
import time
import requests

# Define your API list file and response path
res_path = 'responses'
date = datetime.date.today()
backup_path = os.path.join("Waze_retrival", str(date), res_path)
if not os.path.exists(backup_path):
    os.makedirs(backup_path)

# Read the API list from a file
file1 = open('area_url.txt', 'r')
apilist = [line.strip() for line in file1 if line.strip()]
file1.close()

# Define the cronJob function
def cronJob():
    for url in apilist:
        try:
            response = requests.get(url)

            # Check for non-200 status code
            if response.status_code != 200:
                print('Failed to retrieve data with status code {}'.format(response.status_code))
                continue  # Skip to the next API call instead of exiting

            data = response.json()

            # Process the data
            if "alerts" in data:
                alert_data = data['alerts']
                if alert_data:
                    unique_filename = str(uuid.uuid4())
                    with open(os.path.join(backup_path, unique_filename + ".csv"), 'w', newline='', encoding="utf-8") as data_file:
                        csv_writer = csv.writer(data_file)
                        columns = list({column for row in alert_data for column in row.keys()})
                        csv_writer.writerow(columns)
                        for row in alert_data:
                            csv_writer.writerow([row.get(column, None) for column in columns])

        except requests.RequestException as e:
            print('Error occurred during the HTTP request: {}'.format(e))
            continue
        except Exception as e:
            print('An unexpected error occurred: {}'.format(e))
            continue

# Schedule the cron job
schedule.every(2).minutes.do(cronJob)

# Run the scheduler in a loop
while True:
    schedule.run_pending()
    time.sleep(1)
