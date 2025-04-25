#!/usr/bin/env python3
import os
import pandas as pd
from azure.storage.blob import ContainerClient
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────
# Make sure these environment variables are set:
#   STORAGE_ACCOUNT, STORAGE_KEY, CONTAINER_NAME
#   DB_USER, DB_PASS, DB_SERVER, DB_NAME

STORAGE_ACCOUNT   = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY       = os.getenv("STORAGE_KEY")
CONTAINER_NAME    = os.getenv("CONTAINER_NAME")

DB_USER           = os.getenv("DB_USER")
DB_PASS           = os.getenv("DB_PASS")
DB_SERVER         = os.getenv("DB_SERVER")   # e.g. retailsqlsrv29.database.windows.net
DB_NAME           = os.getenv("DB_NAME")     # e.g. retail_db

# ─── STEP 1: DOWNLOAD ALL BLOBS ─────────────────────────────────────────────────
def download_blobs():
    print("Step 1: Downloading blobs from Azure Storage…")
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={STORAGE_ACCOUNT};"
        f"AccountKey={STORAGE_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    client = ContainerClient.from_connection_string(conn_str, container_name=CONTAINER_NAME)

    for blob in client.list_blobs():
        local_path = os.path.join("data", "raw", blob.name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"  • {blob.name} → {local_path}")
        with open(local_path, "wb") as f:
            stream = client.download_blob(blob)
            f.write(stream.readall())


# ─── HELPERS TO FIND CSV FILES ───────────────────────────────────────────────────
def discover_files():
    base = os.path.join("data", "raw")
    files = {
        "households":   None,
        "transactions": None,
        "products":     None,
    }
    for root, _, names in os.walk(base):
        for fn in names:
            path = os.path.join(root, fn)
            if fn.lower().endswith("households.csv"):
                files["households"] = path
            elif fn.lower().endswith("transactions.csv"):
                files["transactions"] = path
            elif fn.lower().endswith("products.csv"):
                files["products"] = path

    missing = [k for k,v in files.items() if v is None]
    if missing:
        raise FileNotFoundError(f"Could not discover files for: {missing}")
    return files["households"], files["transactions"], files["products"]


# ─── STEP 2–5: LOAD INTO AZURE SQL ───────────────────────────────────────────────
def load_into_sql():
    print("\nStep 2–5: Loading data into Azure SQL…")
    h_file, t_file, p_file = discover_files()
    print("Files to load:")
    print(f"  Households:   {h_file}")
    print(f"  Transactions: {t_file}")
    print(f"  Products:     {p_file}")

    # read CSVs
    df_h = pd.read_csv(h_file)
    df_t = pd.read_csv(t_file)
    df_p = pd.read_csv(p_file)

    # parse any date columns in transactions (if present)
    for c in df_t.columns:
        if "date" in c.lower() or "purchase" in c.lower():
            df_t[c] = pd.to_datetime(df_t[c], errors="coerce")

    # build connection URL
    pwd = quote_plus(DB_PASS)
    conn_url = (
        f"mssql+pymssql://{DB_USER}:{pwd}@{DB_SERVER}/{DB_NAME}"
    )
    engine = create_engine(conn_url)

    # test connectivity
    print("\nConnection test:", engine.connect().closed == False)

    # write each DataFrame in small batches
    for name, df in [
        ("households",   df_h),
        ("transactions", df_t),
        ("products",     df_p),
    ]:
        print(f"\nWriting {name}…")
        df.to_sql(
            name, engine,
            if_exists="replace", index=False,
            method="multi",
            chunksize=500     # ← much smaller batches
        )
    print("\nAll tables written successfully.")


# ─── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    download_blobs()
    load_into_sql()
