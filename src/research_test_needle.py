from research_queries import get_mysql_engine, get_mongo_collection_local, get_mongo_collection_atlas
import time
from sqlalchemy import text 

# The specific "Needle" we are looking for
TARGET_ID = "5SuOikwiRyPMVoIQDJUgSV"

def find_one_mysql(engine):
    # CHANGED: 'id' to 'track_id' to match your actual column name
    query = text("SELECT * FROM spotify_tracks WHERE track_id = :target_id")
    with engine.connect() as connection:
        result = connection.execute(query, {"target_id": TARGET_ID})
        data = result.fetchone() 
        return data
    
def find_one_mongo(collection):
    # Standard MongoDB single-document lookup
    return collection.find_one({"track_id": TARGET_ID})

if __name__ == "__main__":
    mysql_eng = get_mysql_engine()
    mongo_loc = get_mongo_collection_local()
    mongo_atl = get_mongo_collection_atlas()

    targets = [
        ("MySQL Local", mysql_eng, find_one_mysql),
        ("Mongo Local", mongo_loc, find_one_mongo),
        ("Mongo Atlas", mongo_atl, find_one_mongo)
    ]

    print(f"{'Database':<15} | {'Time (s)':<12} | {'Status'}")
    print("-" * 45)

    for name, conn, func in targets:
        start = time.time()
        result = func(conn)
        elapsed = time.time() - start
        
        # Check if we actually found it
        status = "Found" if result else "NOT FOUND"
        print(f"{name:<15} | {elapsed:<12.6f} | {status}")