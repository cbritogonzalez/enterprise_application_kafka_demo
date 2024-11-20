import requests

def fetch_streaming_weather(token):
    print("fetching streaming data...")
    base_url = f'http://api.weatherapi.com/v1/current.json'
    params = {
        "key": token,
        "q": "Groningen"
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error fetching information. {response.status_code} {response.reason}")
        return None
