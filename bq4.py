from google.cloud import bigquery
SERVICE_ACCOUNT_JSON=r'C:\Users\saikr\Downloads\modular-asset-332406-b566e60f8085.json'

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
tab_id="modular-asset-332406.py_dset.consolegames"
table_id="modular-asset-332406.py_dset.consoledates"
job_config=bigquery.LoadJobConfig(autodetect=True,
                                  source_format=bigquery.SourceFormat.CSV,
                                  skip_leading_rows=1
                                  )
job_config1=bigquery.LoadJobConfig(autodetect=True,
                                  source_format=bigquery.SourceFormat.CSV,
                                  skip_leading_rows=1
                                  )
file_path=r'C:\Users\saikr\Downloads\P9-ConsoleGames.csv'
file_path1=r'C:\Users\saikr\Downloads\P9-ConsoleDates.csv'
source_file=open(file_path,"rb")
job=client.load_table_from_file(source_file,tab_id,job_config=job_config)
source_file=open(file_path1,"rb")
job1=client.load_table_from_file(source_file,table_id,job_config=job_config1)
job.result()
job1.result()
table=client.get_table(tab_id)
table2=client.get_table(table_id)
print("loaded {} rows in {}".format(table.num_rows, tab_id))
print("loaded {} rows in {}".format(table2.num_rows, table_id))

