import pandas as pd
import requests
import io
import os

url = "https://storage.googleapis.com/challenge_junior/categoria.parquet"

save_path = "/opt/data/categoria.parquet"

response = requests.get(url)

if response.status_code == 200:
    file_stream = io.BytesIO(response.content)
    df = pd.read_parquet(file_stream)
    
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    df.to_parquet(save_path)
    
    print(f"Arquivo salvo em: {save_path}")
else:
    print(f"Erro ao baixar o arquivo: {response.status_code}")