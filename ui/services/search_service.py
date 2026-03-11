import requests

API_URL = "http://localhost:8000"

def search_cars(filters):
    params = {
        "make": filters.get("make_name"),
        "model": filters.get("model_name"),
        "year": filters.get("made_year"),
        "city": filters.get("auction_city"),
        "date": filters.get("auction_date"),
    }
    params = {k: v for k, v in params.items() if v}  # remove empty values
    
    response = requests.get(f"{API_URL}/auctions", params=params)
    response.raise_for_status()
    return response.json()