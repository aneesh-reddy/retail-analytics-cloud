# load.py

import os
from azure.storage.blob import ContainerClient
import pandas as pd
from sqlalchemy import create_engine

# 1) Azure Blob config
CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER_NAME    = "rawdata"    # ← your container name

def download_blobs():
    client = ContainerClient.from_connection_string(
        CONNECTION_STRING,
        container_name=CONTAINER_NAME
    )
    for blob in client.list_blobs():
        local_path = os.path.join("data", "raw", blob.name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            download_stream = client.get_blob_client(blob).download_blob()
            f.write(download_stream.readall())
    print("Blobs downloaded.")

def load_into_sql():
    # read only the first 10k transaction rows
    df_t = pd.read_csv(
        "data/raw/400_transactions.csv",
        parse_dates=["PURCHASE_"],
        nrows=10000
    )
    df_h = pd.read_csv("data/raw/400_households.csv")
    df_p = pd.read_csv("data/raw/400_products.csv")

    # (Optional) clean up column names here...

    engine = create_engine(os.environ["AZURE_SQL_CONNECTION_STRING"])
    with engine.begin() as conn:
        df_h.to_sql("households",   conn, if_exists="replace", index=False)
        df_t.to_sql("transactions", conn, if_exists="replace", index=False, chunksize=5000, method="multi")
        df_p.to_sql("products",     conn, if_exists="replace", index=False)
    print("Data loaded into Azure SQL.")

if __name__ == "__main__":
    print("Step 1: Downloading blobs from Azure Storage…")
    download_blobs()
    print("Step 2–5: Loading data into Azure SQL…")
    load_into_sql()
