from flask import Flask,render_template,request
import numpy as np
import math
import json
from google.cloud import storage
import heapq

app = Flask(__name__)
data = {}
most_popular_movies = {}
movie_images = {}

@app.route('/')
def index():
    global data
    global most_popular_movies
    global movie_images
    data,most_popular_movies,movie_images = get_data()
    # print(data.keys())
    return render_template('index.html')

@app.route('/suggestions', methods = ['GET'])
def suggestion():
    userid = request.args.get('userid')
    # print(userid)
    # print(data[userid])
    li = get_movie_images_list(get_suggestions(userid))
    li2 = get_movie_images_list(get_top_suggestions())

    # return li[0]
    return render_template('rec_movies.html', list = li, list2 = li2, len1 = len(li), len2 = len(li2))
    #return userid

@app.route('/topsuggestions', methods = ['GET'])
def topsuggestion():
    # li2 = get_movie_images_list(get_top_suggestions())
    li2 = suggestion_help([],15)
    return render_template('top_movies.html', list2 = li2, len2 = len(li2))


# get movie names and images from movie names
def get_movie_images_list(li):
    result = []
    for movie in li:
        temp = []
        temp.append(movie)
        temp.append(movie_images[movie])
        result.append(temp)
    return result

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

# Get data from bucket
def get_data():
    data = download_json('json_data_deep_suchak96', 'temp.json')
    most_popular_movies = download_json('json_data_deep_suchak96', 'top_movies.json')
    movie_images = download_json('json_data_deep_suchak96', 'movie_images.json')
    return data,most_popular_movies, movie_images

def string_to_float(str):
    if len(str) < 1:
        return 0.0
    return float(str)

def sim(a,b):
    n = mean_a = mean_b = 0.0
    # print(data[a])
    # print(data[b])
    if len(data[a])>len(data[b]):
        for movie in data[b]:
            if movie in data[a]:
                mean_a += string_to_float(data[a][movie])
                mean_b += string_to_float(data[b][movie])
                n += 1
    else:
        for movie in data[a]:
            if movie in data[b]:
                mean_a += string_to_float(data[a][movie])
                mean_b += string_to_float(data[b][movie])
                n += 1
       
    if n == 0:
        return 0 
    
    mean_a = mean_a/n
    mean_b = mean_b/n
    numerator = cs_b = cs_a = 0.0
    
    if len(data[a])>len(data[b]):
        for movie in data[b]:
            if movie in data[a]:
                numerator = numerator + ((string_to_float(data[a][movie])-mean_a)*(string_to_float(data[b][movie])-mean_b))
                cs_a = cs_a + ((string_to_float(data[a][movie])-mean_a) * (string_to_float(data[a][movie])-mean_a))
                cs_b = cs_b + ((string_to_float(data[b][movie])-mean_b) * (string_to_float(data[b][movie])-mean_b))
    else:
        for movie in data[a]:
            if movie in data[b]:
                numerator = numerator + ((string_to_float(data[a][movie])-mean_a)*(string_to_float(data[b][movie])-mean_b))
                cs_a = cs_a + ((string_to_float(data[a][movie])-mean_a) * (string_to_float(data[a][movie])-mean_a))
                cs_b = cs_b + ((string_to_float(data[b][movie])-mean_b) * (string_to_float(data[b][movie])-mean_b))
    
    if cs_a == 0 or cs_b == 0:
        return 0
    
    similarity = numerator/(math.sqrt(cs_a)*math.sqrt(cs_b))
    return similarity

def get_top_suggestions():
    max = 15
    top_movies = heapq.nlargest(max, most_popular_movies, key=most_popular_movies.get)
    return top_movies

def get_suggestions(query):
    max = 15
    similar_users = find_sim_users(query)
    # print(similar_users)
    suggested_movie = []
    if len(similar_users) == 0:
        suggested_movie = suggestion_help(suggested_movie,max)
        
    for name in similar_users:
        for movie in data[name]:
                if string_to_float(data[name][movie]) == 5.0 and movie not in suggested_movie and movie in most_popular_movies and movie not in data[query]:
                    if len(suggested_movie) == max:
                        break
                    suggested_movie.append(movie)

                        
    if len(suggested_movie) < max:
        suggested_movie = suggestion_help(suggested_movie,(max-len(suggested_movie)))
    return suggested_movie

def suggestion_help(suggested_movie,max):
    x = 0
    for movie in most_popular_movies:
        if x == max:
            break
        suggested_movie.append(movie)
        x += 1
    return suggested_movie

def find_sim_users(query):
    a = {}
    for name2 in data:
        sim_score = sim(query, name2)
        a[name2] = sim_score
    return heapq.nlargest(7, a.keys(), key=a.get) 


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9090)
