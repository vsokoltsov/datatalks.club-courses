import requests

if __name__ == '__main__':
    for i in range(1, 20):
        response = requests.get("https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api", params={"page": i, "per_page": 100 })
        json_response = response.json()
        print(len(json_response))
