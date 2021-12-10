from google.cloud import bigquery
SERVICE_ACCOUNT_JSON=r'C:\Users\saikr\Downloads\modular-asset-332406-b566e60f8085.json'

client=bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

dataset_id="modular-asset-332406.py_dset"
dataset=bigquery.Dataset(dataset_id)
dataset.location="US"
dataset.description="python dataset"

dataset_ref= client.create_dataset(dataset,timeout=30)
print("{} is created in {}".format(dataset_ref.dataset_id,client.project))
