import requests
import json
import boto3
from datetime import datetime

# 1. Setup the Connection to MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",      
    aws_access_key_id="admin",                
    aws_secret_access_key="password123",      
)

def fetch_and_upload():
    # 2. Fetch data from CoinGecko (Much more reliable!)
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true"
    
    print("Step 1: Fetching data from CoinGecko...")
    response = requests.get(url)
    data = response.json()
    
    # 3. Add our tracking metadata
    data['bitcoin']['ingested_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data['bitcoin']['source'] = "coingecko"

    # 4. Create a unique filename
    filename = f"bitcoin_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # 5. Upload to MinIO
    print(f"Step 2: Uploading {filename} to MinIO...")
    s3.put_object(
        Bucket="raw-data",
        Key=filename,
        Body=json.dumps(data)
    )
    print(f" Success! {filename} is now in your Data Lake.")

if __name__ == "__main__":
    fetch_and_upload()
