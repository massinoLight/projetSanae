from google.cloud import bigquery
from google.cloud import storage

# Replace 'kaggle-competitions-project' with YOUR OWN project id here --
PROJECT_ID = 'projet-sanae' #
#PROJECT_ID='kaggle-competitions-project'


client = bigquery.Client()
table="projet-sanae.BigQueryGeotabIntersectionCongestion.table_train"


# recupérer la frequence des status des critycity k1...k4
def get_data_from_gcp():
    bqclient = bigquery.Client()
    table="projet-sanae.BigQueryGeotabIntersectionCongestion.table_train"
    # Download query results.
    query = "SELECT *" \
            "FROM   "+table



    query_job = bqclient.query(query)

    dataframe = (
        query_job
            .result()
            .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    return dataframe

#Create table from bucket file to big query
def upload_to_bigquery_append(uri):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    table_id = "projet-sanae.BigQueryGeotabIntersectionCongestion.table_train"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        write_disposition="WRITE_APPEND"
    )


    #uri = "gs://bucketki/FOREVERZONE"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()  # Wait for the job to complete.
    print("upload succes")



def upload_to_bucket_append(blob_name, path_to_file):
    """ Upload data to a bucket"""

    storage_client = storage.Client(PROJECT_ID)
    buckets = list(storage_client.list_buckets())
    print(buckets)
    bucket = storage_client.get_bucket("bigquery_geotab_intersection_congestion")
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    uri="gs://bigquery_geotab_intersection_congestion/"+blob_name
    upload_to_bigquery_append(uri)



def drop_duplicate():
        client = bigquery.Client()

        query_text = f"""
                   CREATE OR REPLACE TABLE  `{table}`
                    AS
                    SELECT
                    DISTINCT *
                    FROM `{table}`

                   """
        query_job = client.query(query_text)
        query_job.result()
        print()


dataframe=get_data_from_gcp()
print(dataframe)
for i in range (0,10):
    df = dataframe.sample()
    print(df)
    df.to_csv('./temp/temp.csv', index=False)
    try:
        upload_to_bucket_append('row', './temp/temp.csv')
    except:
        continue

print("10 cols ajoutés avec succe")

drop_duplicate()

print("la redondance est surpprimée")