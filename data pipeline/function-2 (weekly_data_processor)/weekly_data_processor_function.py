import base64
import csv
import json
from google.cloud import storage

    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    # print(pubsub_message)

# Upload json to file in bucket
def upload_json_to_file(bucket_name, filename, data):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(filename)
    blob.upload_from_string(data=json.dumps(data), content_type='application/json')

# convert json to dict
def download_json(bucket_name, filename):
    string = download_file(bucket_name, filename)
    if len(string) < 4:
        return {}
    return json.loads(string)

# Download file and return as string
def download_file(bucket_name, filename):
    
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(filename)
    if blob is None:
        return ''
    downloaded_blob = blob.download_as_string().decode("utf-8")
    return downloaded_blob

# Process the tabular data and covert to dict, eliminate extra cols.
# Dict: {User_id:{movie_title:rating_score}}
def data_process(data_to_process, data, movie_frequency, movie_images):
    for tupple in data_to_process:

        movie_images[tupple["movie_title"]] = tupple["movie_image_url"] 

        rating = tupple["rating_score"]
        if tupple["user_id"] not in data:     
            data[tupple["user_id"]] = {tupple["movie_title"]: rating}
        else:
            data[tupple["user_id"]][tupple["movie_title"]] = rating

        if tupple["movie_title"] in movie_frequency:
            movie_frequency[tupple["movie_title"]] = movie_frequency[tupple["movie_title"]] + 1
        else:
            movie_frequency[tupple["movie_title"]] = 1
    return data,movie_frequency,movie_images

# Get the downloaded string and return as processed dict.
# Dict: {User_id:{movie_title:rating_score}}
def file_to_dict(file_as_string, process_data, movie_frequency, movie_images):
    splits = file_as_string.split('\n')
    labels = splits[0].split(',')
    data = []
    for i in range(1,len(splits)-1):
        data_dict = {}
        split_row = splits[i].split(',')
        for i in range(1,len(split_row)):
            data_dict[labels[i]] = split_row[i]
        data.append(data_dict)

    return data_process(data, process_data, movie_frequency, movie_images)




def split_pubsub(event, context):
    
    downloaded_file = download_file('split_movie_data_deep_suchak96_1',event['attributes']['filename'])
    process_data = {}
    movie_frequency = {}

    process_data = download_json('json_data_deep_suchak96','temp.json')
    movie_frequency = download_json('json_data_deep_suchak96','temp_movie.json')
    movie_images = download_json('json_data_deep_suchak96','movie_images.json')
    
    process_data,movie_frequency,movie_images = file_to_dict(downloaded_file, process_data, movie_frequency, movie_images)
    # process_data,movie_frequency = file_to_dict(downloaded_file, process_data, movie_frequency)
    most_popular_movies = dict(sorted(movie_frequency.items(), key=lambda kv: kv[1], reverse=True)[:100])

    upload_json_to_file('json_data_deep_suchak96','movie_images.json', movie_images)
    upload_json_to_file('json_data_deep_suchak96','top_movies.json', most_popular_movies)
    upload_json_to_file('json_data_deep_suchak96','temp.json', process_data)
    upload_json_to_file('json_data_deep_suchak96','temp_movie.json', movie_frequency)
