import pandas as pd
import os

# Load CSV
df = pd.read_csv('/home/jovyan/work/data/listings_Paris.csv', low_memory=False)


# Create /data directory if missing
os.makedirs('./data', exist_ok=True)

# 1. Generate movies.redis file (listing info)
with open('./data/movies.redis', 'w', encoding='utf-8') as f:
    for _, row in df.iterrows():
        movie_key = f"movie:{row['id']}"
        f.write(f"HMSET {movie_key} name \"{row['name']}\" description \"{row['description']}\" "
                f"neighbourhood \"{row.get('neighbourhood_overview', '')}\" host_id {row['host_id']}\n")

# 2. Generate actors.redis file (host info)
hosts = df[['host_id', 'host_name', 'host_about']].drop_duplicates()

with open('./data/actors.redis', 'w', encoding='utf-8') as f:
    for _, row in hosts.iterrows():
        actor_key = f"actor:{row['host_id']}"
        name = str(row['host_name']).replace('"', '')
        about = str(row['host_about']).replace('"', '')
        f.write(f"HMSET {actor_key} name \"{name}\" about \"{about}\"\n")

print(" Redis files generated successfully.")
