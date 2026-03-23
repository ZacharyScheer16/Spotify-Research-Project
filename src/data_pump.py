import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pymongo import MongoClient

load_dotenv()


def load_and_clean_data(file_path):

    df = pd.read_csv(file_path, index_col=0)

# Remove rows with missing values (Standard practice)
    df = df.dropna()

    print(f"Data loaded and cleaned. length of data: {len(df)}")
    return df


def push_to_mySql(df):
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT")
    database = os.getenv("MYSQL_DATABASE")

    conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    

    engine = create_engine(conn_str, echo=False)
    df.to_sql("spotify_tracks", con=engine, if_exists="replace", index=False)
    print("Data pushed to MySQL successfully.")

def push_to_mongo_local(df):
    uri = os.getenv("MONGO_LOCAL_URI")
    client = MongoClient(uri)

    # 2. Access the database and collection
    # Note: Mongo creates these automatically if they don't exist!
    db = client[os.getenv("DB_NAME")]
    collection = db["spotify_tracks"]

    #3. Transform: Convert to data frame to dictionary format for MongoDB
    print("Transforming data for MongoDB...")
    data_dict = df.to_dict(orient="records")

    #4. Load: Insert data into MongoDB
    collection.delete_many({})  # Clear existing data (optional)
    collection.insert_many(data_dict)

    print("Data pushed to MongoDB successfully.")

###########################################################
###########################################################

def push_to_mongo_online(df):
    uri = os.getenv("MONGO_ATLAS_URI")
    client = MongoClient(uri)

    # 2. Access the database and collection
    # Note: Mongo creates these automatically if they don't exist!
    db = client[os.getenv("DB_NAME")]
    collection = db["spotify_tracks"]

    #3. Transform: Convert to data frame to dictionary format for MongoDB
    print("Transforming data for MongoDB...")
    data_dict = df.to_dict(orient="records")

    #4. Load: Insert data into MongoDB
    collection.delete_many({})  # Clear existing data (optional)
    collection.insert_many(data_dict)

    print("Data pushed to MongoDB successfully.")


import time
if __name__ == "__main__":
    # 1. Extract & Transform
    file = "Spotify_dataset.csv"
    df = load_and_clean_data(file)

    print("\n--- Starting Benchmarks ---")

    # Time the MySQL Push
    start_mysql = time.time()
    push_to_mySql(df)
    mysql_time = time.time() - start_mysql
    print(f"MySQL (Local) took: {mysql_time:.2f} seconds")

    # Time the MongoDB Local Push
    start_mongo_local = time.time()
    push_to_mongo_local(df)
    mongo_local_time = time.time() - start_mongo_local
    print(f"MongoDB (Local) took: {mongo_local_time:.2f} seconds")

    # Time the MongoDB Atlas (Cloud) Push
    start_atlas = time.time()
    push_to_mongo_online(df)
    atlas_time = time.time() - start_atlas
    print(f"MongoDB Atlas (Cloud) took: {atlas_time:.2f} seconds")

    print("\n--- Final Results ---")
    print(f"SQL vs Mongo Local: {abs(mysql_time - mongo_local_time):.2f}s difference")
    print(f"Cloud Latency Penalty: {atlas_time - mongo_local_time:.2f}s extra for Atlas")
