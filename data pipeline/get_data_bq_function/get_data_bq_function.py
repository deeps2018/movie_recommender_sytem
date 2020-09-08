from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud import storage
import pandas as pd
import os
import datetime


def execute_bq(today):
    client = bigquery.Client()

    yesterday = today - datetime.timedelta(days = 7)
    date = today.strftime('%Y-%m-%d')
    date2 = yesterday.strftime('%Y-%m-%d')

    query = """
    SELECT rating_timestamp_utc, A.movie_id as movie_id, B.movie_title as movie_title, B.movie_image_url as movie_image_url, user_id, rating_score
    FROM `sunlit-inquiry-286015.mubi_dataset.mubi_ratings_data` as A, `sunlit-inquiry-286015.mubi_dataset.mubi_movie_data` as B 
    WHERE rating_timestamp_utc >= "{}" AND rating_timestamp_utc <= "{}" 
    AND A.movie_id=B.movie_id 
    Order by rating_timestamp_utc
    """.format(date2,date)

    query_job = client.query(query)
    data_in_df = query_job.result().to_dataframe()
    for i in range(len(data_in_df)):
        data_in_df['movie_title'][i] = data_in_df['movie_title'][i].replace(',','_')

    return data_in_df


def load_result_to_bucket(query_result_df, today):
    date = (today - datetime.timedelta(days = 7)).strftime('%Y-%m-%d')
    bucket_name = 'split_movie_data_deep_suchak96_1'
    filename = "splitdata/data_"+date+".csv"

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
        
    bucket.blob(filename).upload_from_string(query_result_df.to_csv(), 'text/csv')
    print('Data uploaded to bucket in file {} '.format(filename))
    return filename


def send_pubsub_message(filename):
    project = "sunlit-inquiry-286015"

    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=project,
        topic='split_file_add',
    )
    publisher.publish(topic_name, b'file added to bucket', filename=filename)

def get_data_from_bq(request):
    
    today = datetime.datetime.today()
    query_result_df = execute_bq(today)

    if len(query_result_df) > 1:
        filename = load_result_to_bucket(query_result_df,today)
        send_pubsub_message(filename)

        