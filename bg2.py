from google.cloud import bigquery
SERVICE_ACCOUNT_JSON=r'C:\Users\saikr\Downloads\modular-asset-332406-b566e60f8085.json'

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

tab_id="modular-asset-332406.py_dset.table_py1"
job_config=bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name","STRING",mode="NULLABLE"),
        bigquery.SchemaField("gender","STRING",mode="NULLABLE"),
        bigquery.SchemaField("count","STRING",mode="NULLABLE")
        ],
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1
    )
file_path=r'C:\Users\saikr\OneDrive\Documents\yob1881.txt'
source_file=open(file_path,"rb")
job=client.load_table_from_file(source_file,tab_id,job_config=job_config)
job.result()
table=client.get_table(tab_id)
print("loaded {} rows in {}".format(table.num_rows, tab_id))
