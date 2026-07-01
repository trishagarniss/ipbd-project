import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_active_jakarta_id():
    api_key = os.getenv("OPENAQ_API_KEY")
    headers = {"X-API-Key": api_key}
    
    # 1. Cari lokasi Jakarta yang memiliki sensor aktif
    search_url = "https://api.openaq.org/v3/locations?coordinates=-6.2088,106.8456&radius=5000"
    response = requests.get(search_url, headers=headers)
    
    locations = response.json().get('results', [])
    if locations:
        # Ambil ID lokasi pertama yang ditemukan
        loc_id = locations[0]['id']
        print(f"ID Jakarta yang Aktif: {loc_id}")
        
        # 2. Tarik data dari ID tersebut
        data_url = f"https://api.openaq.org/v3/locations/{loc_id}/sensors"
        data_resp = requests.get(data_url, headers=headers)
        print(json.dumps(data_resp.json(), indent=2))
    else:
        print("Tidak ada lokasi aktif ditemukan di koordinat tersebut.")

if __name__ == "__main__":
    get_active_jakarta_id()