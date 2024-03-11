import os
import csv
import uuid
import schedule
import time
from datetime import datetime, date as dt_func
import requests
from newspaper import Article
import argparse

# Define a function to parse command-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description='Script to fetch and process GDELT articles based on provided keywords.')
    parser.add_argument('event_name', type=str, help='Name of the event to be processed')
    parser.add_argument('keywords', type=str, nargs='+', help='Keywords to search for in the GDELT articles')
    return parser.parse_args()

# Main function
def main():
    args = parse_arguments()

    res_path = 'gdelt-responses'
    filename = 'gdelt-file_name'

    print("Event Name Entered:", args.event_name)
    print("Keywords Entered:")
    for keyword in args.keywords:
        print(keyword)

    # New path
    current_dir = os.getcwd()
    backup_path = os.path.join(current_dir, "gdelt_data", str(dt_func.today()), res_path)
    file_name_path = os.path.join(current_dir, "gdelt_data", args.event_name, filename)
  
    # backup_path = os.path.join("../../nlp-earthquake/data", str(dt_func.today()), res_path)
    # file_name_path = os.path.join("../../nlp-earthquake/data", args.event_name, filename)
        
    os.makedirs(backup_path, exist_ok=True)
    os.makedirs(file_name_path, exist_ok=True)

    api_template = "https://api.gdeltproject.org/api/v2/doc/doc?format=html&timespan=6H&query={}&mode=artlist&maxrecords=250&format=json&sort=hybridrel"
    apilist = [api_template.format(keyword) for keyword in args.keywords]

    outF = open(os.path.join(file_name_path, "myOutFile.txt"), "a")

    # Define the cron job function
    def cronJob():
        for api_url in apilist:
            try:
                print(f"Fetching data from: {api_url}")
                response = requests.get(api_url)
                if response.status_code != 200:
                    print(f'Failed to retrieve articles with error {response.status_code}')
                    continue

                try:
                    data = response.json()
                except ValueError:
                    print("Failed to decode JSON from response")
                    continue

                articles_data = data.get("articles", [])
                print(f"Found {len(articles_data)} articles")

                if articles_data:
                    unique_filename = str(uuid.uuid4())
                    outF.write(unique_filename + "\n")
                    with open(os.path.join(backup_path, unique_filename + ".csv"), 'w', encoding="utf-8", newline='') as data_file_alert:
                        csv_writer = csv.DictWriter(data_file_alert, fieldnames=["url", "url_mobile", "title", "publishedAt", "urlToImage", "name", "language", "sourcecountry", "content", "id", "author", "retrieve_time", "context"])
                        csv_writer.writeheader()

                        for article_data in articles_data:
                            article_data['retrieve_time'] = datetime.now().isoformat()
                            url = article_data["url"]
                            try:
                                article = Article(url.strip())
                                article.download()
                                article.parse()
                                article_text = article.text.replace('\n', ' ')
                                article_data["context"] = article_text[:300]
                            except Exception as e:
                                print(f"Failed to process article at {url}: {e}")
                            
                            # Filter article_data to include only the specified fields
                            filtered_article_data = {key: article_data[key] for key in csv_writer.fieldnames if key in article_data}
                            csv_writer.writerow(filtered_article_data)
            except Exception as e:
                print(f"An error occurred: {e}. Continuing with next API call.")


    # Schedule the cron job
    schedule.every(2).minutes.do(cronJob)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
