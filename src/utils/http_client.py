import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()


class Spotify81Client:
    
    def __init__(self):
        self.api_key = os.getenv("RAPIDAPI_KEY")
        self.host = "spotify81.p.rapidapi.com"
        
        #kiểm tra env có tồn tại
        if not self.api_key:
            raise ValueError("[LOI]: Không tìm thấy RAPIDAPI_KEY")
        
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.host,
            "Content-Type": "application/json"
        }
    
    #hàm gửi yêu cầu
    def get(self, endpoint, params=None, max_retries=3):
        
        url = f"https://{self.host}{endpoint}"
        
        for attempt in range(max_retries):
            try:
                #gửi yêu cầu lên server
                response = requests.get(url=url, headers=self.headers, params=params, timeout=15)
                
                if response.status_code == 200:
                    return response.json()
                
                elif response.status_code == 429:
                    print(f"[LOI] Server đang quá tải (lỗi 429). Nghỉ 15s... (Lần thử {attempt + 1}/{max_retries})")
                    time.sleep(15)
                    
                #nếu sai key hoặc đạt limit của API
                elif response.status_code == 403:
                    print(f"[LOI] Lỗi API ({response.status_code}): {response.text}")
                    break
            
            except requests.exceptions.RequestException as e:
                print(f"[LOI] Lỗi mạng: {e}. Đang thử lại...")
                time.sleep(5)
                
        return None