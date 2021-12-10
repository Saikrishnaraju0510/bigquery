from google.cloud import bigquery
SERVICE_ACCOUNT_JSON=r'C:\Users\saikr\Downloads\modular-asset-332406-b566e60f8085.json'

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

query="""SELECT * FROM modular-asset-332406.py_dset.table_py1
         WHERE name LIKE 'M%' """

query_job=client.query(query)

print(query_job)
print("query completed")
for i in query_job:
    print(str(i[0])+ "," + str(i[1]) + "," + str(i[1]))
