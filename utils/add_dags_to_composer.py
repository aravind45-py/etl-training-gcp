import os
from google.cloud import storage

def upload_files_to_gcs(bucket_name, source_folder, destination_folder):
    """Uploads all files from a local directory to a GCS bucket folder."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    print(f"Uploading files from {source_folder} to gs://{bucket_name}/{destination_folder}...")

    for filename in os.listdir(source_folder):
        local_path = os.path.join(source_folder, filename)
        if os.path.isfile(local_path):
            # Ensure the blob path uses forward slashes for GCS
            blob_path = os.path.join(destination_folder, filename).replace('\\', '/')
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)
            print(f"Successfully uploaded {filename} to {blob_path}")

if __name__ == "__main__":
    # Configuration: Update BUCKET_NAME with your specific GCS bucket
    BUCKET_NAME = "etl-training-bk-dev"
    SOURCE_DIR = "/home/rajagopal_aravind45/etl_project_gcp/src/dags/"
    DEST_FOLDER = "dags"  # Destination folder in the bucket
    upload_files_to_gcs(BUCKET_NAME, SOURCE_DIR, DEST_FOLDER)