# import json
# import gzip
# from minio import Minio
# from config.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKET_NAME

# def ingest_data(rankings_data, movies_data):
#     client = Minio(
#         MINIO_ENDPOINT,
#         access_key=MINIO_ACCESS_KEY,
#         secret_key=MINIO_SECRET_KEY,
#         secure=False
#     )

#     # Ensure bucket exists
#     if not client.bucket_exists(BUCKET_NAME):
#         client.make_bucket(BUCKET_NAME)

#     # Ingest rankings data
#     rankings_path = "raw/rankings.json.gz"
#     rankings_data_compressed = gzip.compress(json.dumps(rankings_data).encode('utf-8'))
#     client.put_object(BUCKET_NAME, rankings_path, rankings_data_compressed, len(rankings_data_compressed))

#     # Ingest movies data
#     movies_path = "raw/movies.json.gz"
#     movies_data_compressed = gzip.compress(json.dumps(movies_data).encode('utf-8'))
#     client.put_object(BUCKET_NAME, movies_path, movies_data_compressed, len(movies_data_compressed))

# if __name__ == "__main__":
#     with open("Bigdata.json", "r") as f:
#         rankings_data = json.load(f)
    
#     with open("film.json", "r") as f:
#         movies_data = json.load(f)
    
#     ingest_data(rankings_data, movies_data)
