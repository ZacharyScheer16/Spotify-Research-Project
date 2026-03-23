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
    