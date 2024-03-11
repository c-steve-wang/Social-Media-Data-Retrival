import os
from datetime import datetime, timedelta
import requests
import pandas as pd
import uuid
import argparse
from datetime import date as dt_func
import schedule
import time

def cronJob(backup_path, file_name_path, keywords, api_key_list):
    print("Cron job started at:", datetime.now())

    end_date = (datetime.utcnow() - timedelta(seconds=1)).replace(second=0).isoformat(timespec='seconds')
    start_date = (datetime.utcnow() - timedelta(minutes=30)).replace(second=0).isoformat(timespec='seconds')
    print(f"Fetching articles from {start_date} to {end_date}")

    url_list = ['https://newsapi.org/v2/everything?q=' + keyword + '&from=' + start_date + 'to=' + end_date + '&sortBy=time&apiKey=' for keyword in keywords]

    final_response_list = []
    num_list = []

    for url in url_list:
        print("Processing URL:", url)
        for api_key in api_key_list:
            try:
                full_url = url + api_key
                print("Trying API Key:", api_key)
                response = requests.get(full_url)
                if response.status_code == 200:
                    articles = response.json().get('articles', [])
                    print(f"Found {len(articles)} articles")
                    if articles:
                        break  # Break if articles are found
                else:
                    print(f"API Request failed with status code: {response.status_code}, message: {response.text}")
            except Exception as e:
                print(f"Exception occurred: {e}")

        processed_articles = []
        for article in articles:
            processed_article = {}
            for key, value in article.items():
                if key == 'source' and isinstance(value, dict):
                    processed_article[key] = value.get('name', 'NA')  # Extract just the name from the source
                else:
                    processed_article[key] = value if value is not None else 'NA'
            processed_articles.append(processed_article)

        final_response_list.extend(processed_articles)
        num_list.append(len(processed_articles))

    if final_response_list:
        df = pd.DataFrame(final_response_list)
        df['retrieve_time'] = datetime.now()
        unique_filename = str(uuid.uuid4())
        file_path = os.path.join(file_name_path, "myOutFile.txt")
        with open(file_path, "a") as outF:
            outF.write(unique_filename + "\n")
        print(f"File updated: {file_path}")

        data_file_alert = os.path.join(backup_path, unique_filename + ".csv")
        df.to_csv(data_file_alert)
        print(f"Data saved to: {data_file_alert}")

        data_file_alert_num = os.path.join(backup_path, unique_filename + "-num_list.csv")
        pd.DataFrame(num_list).to_csv(data_file_alert_num)
        print(f"Number list saved to: {data_file_alert_num}")
    else:
        print("No articles were processed.")



def main():
    parser = argparse.ArgumentParser(description='Script to fetch news data based on event name and keywords.')
    parser.add_argument('event_name', type=str, help='Event name')
    parser.add_argument('keywords', nargs='+', help='List of keywords')
    args = parser.parse_args()

    res_path = 'newsapi-responses'
    filename = 'newsapi-file_name'
    api_key_list = ['sampl-this-is-a-sample-inside-01', 'key-this-is-a-sample-inside-02', 'api-this-is-a-sample-inside-03','code-this-is-a-sample-inside-04']

    # backup_path = os.path.join("../../nlp-earthquake/data", str(dt_func.today()), res_path)
    # file_name_path = os.path.join("../../nlp-earthquake/data", args.event_name, filename)
    current_dir = os.getcwd()
    backup_path = os.path.join(current_dir, "newsapi_data", str(dt_func.today()), res_path)
    file_name_path = os.path.join(current_dir, "newsapi_data", args.event_name, filename)
    if not os.path.exists(backup_path):
        os.makedirs(backup_path)
    if not os.path.exists(file_name_path):
        os.makedirs(file_name_path)

    schedule.every(10).minutes.do(cronJob, backup_path, file_name_path, args.keywords, api_key_list)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    main()
