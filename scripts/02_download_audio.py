import os
import pandas as pd
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore
import time

csv_file = "../data/intermediate/filter_mood.csv"
save_dir = "../data/raw/audio_mood"
os.makedirs(save_dir, exist_ok=True)

client_id = "c2946d30" 
df = pd.read_csv(csv_file)

sema = Semaphore(3)
 
def download_audio(track_id):
    num_id = int(track_id.replace("track_", ""))

    save_path = os.path.join(save_dir, f"{track_id}.mp3")
    if os.path.exists(save_path):
        return
    url = f"https://api.jamendo.com/v3.0/tracks/file?client_id={client_id}&id={num_id}"

    with sema:
        try:
            r = requests.get(url, stream=True, timeout=30)
            time.sleep(0.3)
            if r.status_code == 200:
                with open(save_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            else:
                print(f"Loi tai {track_id}: {r.status_code}")
        except Exception as e:
            print(f"Loi {track_id}: {e}")

with ThreadPoolExecutor(max_workers=10) as executor:
    list(tqdm(executor.map(download_audio, df["TRACK_ID"]), total=len(df)))
print("Da tai xong tat ca bai nhac co tag cam xuc that.")
