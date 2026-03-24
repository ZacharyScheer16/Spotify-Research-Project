from research_queries import get_mysql_engine, get_mongo_collection_local, get_mongo_collection_atlas
import time
import pandas as pd
from sqlalchemy import create_engine, text
import textwrap


def aggregate_mysql(engine):
    query = text("SELECT track_genre, AVG(popularity) AS avg_popularity_byGenre FROM spotify_tracks " \
    "GROUP BY track_genre ORDER BY track_genre ASC LIMIT 10")
    with engine.connect() as connection:
        result = connection.execute(query)
        data = result.fetchall() 
        return data


def aggregate_mongo(collection):
    # The pipeline is a LIST of STAGES (dictionaries)
    pipeline = [
        {
            "$group": {
                "_id": "$track_genre",
                "avg_popularity": { "$avg": "$popularity" }
            }
        },
        {
            "$sort": { "_id": 1 } 
        },
        {
            "$limit": 10  # This must be its own stage!
        }
    ]
    # Run the aggregation
    result = collection.aggregate(pipeline)
    # .aggregate() returns a cursor, so we cast it to a list to "pull" the data
    return list(result)

if __name__ == "__main__":
    mysql_eng = get_mysql_engine()
    mongo_loc = get_mongo_collection_local()
    mongo_atl = get_mongo_collection_atlas()

    targets = [
        ("MySQL Local", mysql_eng, aggregate_mysql),
        ("MongoDB Local", mongo_loc, aggregate_mongo),
        ("MongoDB Atlas", mongo_atl, aggregate_mongo)
    ]

    for name, target, func in targets:
        start_time = time.time()
        result = func(target)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{name} - Elapsed Time: {elapsed_time:.4f} seconds")
        print(f"{name} - Result Sample: {result[:3]}")  # Print first 3 results for brevity