import pandas as pd
from pymongo import MongoClient
import os

client = MongoClient(f"mongodb://admin:secret@mongo:27017/")
db = client["airbnb_data"]
collection = db["airbnd"]

df = pd.read_csv("/data/listings_Paris.csv")


records = []
for _, row in df.iterrows():
    record = {
        "id": int(row["id"]),
        "name": row.get("name", ""),
        "description": row.get("description", ""),
        "host": {
            "host_name": row.get("host_name", ""),
            "is_superhost": row.get("host_is_superhost", False)
        },
        "city": row.get("city", ""),
        "room_type": row.get("room_type", ""),
        "price": float(row["price"]),
        "number_of_reviews": int(row.get("number_of_reviews", 0)),
        "instant_bookable": row.get("instant_bookable", False),
    }
    records.append(record)

collection.insert_many(records)
print(f"{len(records)} documents insérés ")
