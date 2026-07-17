from azure.storage.blob import BlobServiceClient

connect_str = "UseDevelopmentStorage=true"
container_name = "datasets"
blob_name = "All_Diets.csv"

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

try:
    blob_service_client.create_container(container_name)
    print(f"Container '{container_name}' created.")
except Exception as e:
    print(f"Container may already exist: {e}")

blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

with open(blob_name, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

print(f"Uploaded {blob_name} to container '{container_name}' successfully.")