import requests
from google.cloud import storage

BUCKET = "de-zoomcamp-yellow-2026-lefa"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def upload_to_gcs(bucket_name, destination_blob_name, source_file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

def web_to_gcs_2024_yellow():
    for month in range(1, 13):
        month_str = f"{month:02d}"
        file_name = f"yellow_tripdata_2024-{month_str}.parquet"
        url = f"{BASE_URL}/{file_name}"

        print(f"Downloading {url}")
        r = requests.get(url)

        if r.status_code != 200:
            raise Exception(f"Failed to download {url}")

        with open(file_name, "wb") as f:
            f.write(r.content)

        upload_to_gcs(
            BUCKET,
            f"yellow/2024/{file_name}",
            file_name
        )

        print(f"Uploaded gs://{BUCKET}/yellow/2024/{file_name}\n")

if __name__ == "__main__":
    web_to_gcs_2024_yellow()

