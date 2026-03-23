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

if __name__ == "__main__":
    # 1. Extract & Transform
    file = "Spotify_dataset.csv"
    df = load_and_clean_data(file)
    
    # 2. Load
    push_to_mySql(df)

    push_to_mongo_local(df)

