import os
import time
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pymongo import MongoClient

load_dotenv()

def get_mysql_engine():
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT")
    database = os.getenv("MYSQL_DATABASE")

    conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

    return create_engine(conn_str, echo=False)

def get_mongo_collection_local(): 
    # Points to your local installation (localhost:27017)
    uri = os.getenv("MONGO_LOCAL_URI")
    client = MongoClient(uri)
    db = client[os.getenv("DB_NAME")]
    return db["spotify_tracks"]

def get_mongo_collection_atlas():
    # Points to your Cloud Cluster (AWS N. Virginia)
    uri = os.getenv("MONGO_ATLAS_URI")
    client = MongoClient(uri)
    db = client[os.getenv("DB_NAME")]
    return db["spotify_tracks"]

# --- MySQL Retrieval ---
def search_mysql(engine):
    query = text("SELECT * FROM spotify_tracks WHERE track_genre = 'acoustic' AND danceability > 0.8")
    with engine.connect() as connection:
        # We only time the actual database work
        result = connection.execute(query)
        data = result.fetchall() 
        return data

# --- MongoDB Retrieval ---
def search_mongo(collection):
    query = {
        "track_genre": "acoustic", 
        "danceability": {"$gt": 0.8}
    }
    # list() forces Mongo to actually pull the data now
    return list(collection.find(query))


if __name__ == "__main__": 
    mysql_eng = get_mysql_engine()
    mongo_loc = get_mongo_collection_local()
    mongo_atl = get_mongo_collection_atlas()

    targets = [
        ("MySQL Local", mysql_eng, search_mysql),
        ("Mongo Local", mongo_loc, search_mongo),
        ("Mongo Atlas", mongo_atl, search_mongo)
    ]

    print(f"{'Database':<15} | {'Avg Time (s)':<12} | {'Count':<6}")
    print("-" * 40)

    for name, connection, search_func in targets:
        times = []
        count = 0
        
        # Run 10 times for a scientific average
        for _ in range(10):
            start = time.time()
            results = search_func(connection)
            times.append(time.time() - start)
            count = len(results) # Ensure all find the same number of rows
            
        avg_time = sum(times) / 10
        print(f"{name:<15} | {avg_time:<12.5f} | {count:<6}")