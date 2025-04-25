import os
import glob
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, text

# ── Configuration ───────────────────────────────────────
STORAGE_ACCOUNT = "retailstoreacct2025"
STORAGE_KEY     = os.getenv("STORAGE_KEY")  # export this beforehand
SERVER_NAME     = "retailsqlsrv29"
DB_NAME         = "RetailDB"
DB_USER         = "sqladmin"
DB_PASS         = "YourStrongP@ss!"
# ────────────────────────────────────────────────────────

def download_blobs():
    """Download all CSVs from the rawdata container into data/raw/."""
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={STORAGE_ACCOUNT};"
        f"AccountKey={STORAGE_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    client = BlobServiceClient.from_connection_string(conn_str)
    container = client.get_container_client("rawdata")

    for blob in container.list_blobs():
        local_path = os.path.join("data", "raw", blob.name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"Downloading {blob.name} → {local_path}")
        with open(local_path, "wb") as f:
            f.write(container.download_blob(blob).readall())

def discover_files():
    """Find the three CSVs by their 400_ prefix."""
    files = glob.glob("data/raw/**/*.csv", recursive=True)
    household = next(f for f in files if os.path.basename(f).lower().startswith("400_household"))
    transactions = next(f for f in files if os.path.basename(f).lower().startswith("400_transaction"))
    products = next(f for f in files if os.path.basename(f).lower().startswith("400_product"))
    return household, transactions, products

def load_into_sql():
    """Load the downloaded CSVs into Azure SQL via pymssql."""
    h_file, t_file, p_file = discover_files()
    print("Files to load:")
    print("  Households:   ", h_file)
    print("  Transactions: ", t_file)
    print("  Products:     ", p_file)

    # Read all columns
    df_h = pd.read_csv(h_file)
    df_t = pd.read_csv(t_file)
    df_p = pd.read_csv(p_file)

    # Auto-parse any date-like columns in transactions
    for col in df_t.columns:
        if "DATE" in col.upper() or "PURCHASE" in col.upper():
            df_t[col] = pd.to_datetime(df_t[col], errors="coerce")

    # Build a pymssql connection URL
    conn_url = (
        f"mssql+pymssql://{DB_USER}:{DB_PASS}"
        f"@{SERVER_NAME}.database.windows.net:1433/{DB_NAME}"
    )
    engine = create_engine(conn_url)

    # Test the connection
    with engine.connect() as conn:
        print("Connection test:", conn.execute(text("SELECT 1")).scalar())

    # Bulk load in chunks
    print("Writing households...")
    df_h.to_sql(
        "households", engine,
        if_exists="replace", index=False,
        method="multi", chunksize=5000
    )

    print("Writing transactions...")
    df_t.to_sql(
        "transactions", engine,
        if_exists="replace", index=False,
        method="multi", chunksize=5000
    )

    print("Writing products...")
    df_p.to_sql(
        "products", engine,
        if_exists="replace", index=False,
        method="multi", chunksize=5000
    )

    # Verify row counts
    with engine.connect() as conn:
        for tbl in ["households", "transactions", "products"]:
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
            print(f"{tbl}: {cnt} rows")

if __name__ == "__main__":
    print("Step 1: Downloading blobs from Azure Storage…")
    download_blobs()
    print("\nStep 2–5: Loading data into Azure SQL…")
    load_into_sql()
    print("\n✅ All done!")
