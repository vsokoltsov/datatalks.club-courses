import requests

if __name__ == '__main__':
    url = "http://localhost:8000/v1/predict"
    client = {
        "lead_source": "organic_search",
        "number_of_courses_viewed": 4,
        "annual_income": 80304.0
    }
    response = requests.post(url, json=client).json()
    print(response)