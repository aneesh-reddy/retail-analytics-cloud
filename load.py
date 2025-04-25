import os, glob, pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, event, text

# ── Config ─────────────────────────────────────
STORAGE_ACCOUNT = "retailstoreacct2025"
STORAGE_KEY     = os.getenv("STORAGE_KEY")
SERVER_NAME     = "retailsqlsrv29"
DB_NAME         = "RetailDB"
DB_USER         = "sqladmin"
DB_PASS         = "YourStrongP@ss!"
# ────────────────────────────────────────────────

def download_blobs():
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={STORAGE_ACCOUNT};"
        f"AccountKey={STORAGE_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    svc = BlobServiceClient.from_connection_string(conn_str)
    cont = svc.get_container_client("rawdata")

    for b in cont.list_blobs():
        lp = os.path.join("data", "raw", b.name)
        os.makedirs(os.path.dirname(lp), exist_ok=True)
        print(f"Downloading {b.name} → {lp}")
        with open(lp, "wb") as f:
            f.write(cont.download_blob(b).readall())

def discover_files():
    files = glob.glob("data/raw/**/*.csv", recursive=True)
    h = next(f for f in files if os.path.basename(f).lower().startswith("400_household"))
    t = next(f for f in files if os.path.basename(f).lower().startswith("400_transaction"))
    p = next(f for f in files if os.path.basename(f).lower().startswith("400_product"))
    return h, t, p

def load_into_sql():
    hfile, tfile, pfile = discover_files()
    print("Files:", hfile, tfile, pfile)

    df_h = pd.read_csv(hfile)
    df_t = pd.read_csv(tfile)
    df_p = pd.read_csv(pfile)
    # auto‐parse any date‐like cols in df_t
    for c in df_t.columns:
        if "DATE" in c.upper() or "PURCHASE" in c.upper():
            df_t[c] = pd.to_datetime(df_t[c], errors="coerce")

    # Build engine with zero timeout
    conn_url = (
        f"mssql+pymssql://{DB_USER}:{DB_PASS}"
    f"@{SERVER_NAME}.database.windows.net:1433/{DB_NAME}"
    "?encrypt=require"
    )
    engine = create_engine(
       conn_url
    )

    # Infinite cursor timeout
    @event.listens_for(engine, "before_cursor_execute")
    def infinite_timeout(conn, cursor, stmt, params, ctx, em):
        cursor.timeout = 0

    # **Test** the connection first
    with engine.connect() as conn:
        print("Connection test:", conn.execute(text("SELECT 1")).scalar())

    # Bulk load in chunks
    df_h.to_sql("households",   engine, if_exists="replace", index=False,
                method="multi", chunksize=5000)
    df_t.to_sql("transactions", engine, if_exists="replace", index=False,
                method="multi", chunksize=5000)
    df_p.to_sql("products",     engine, if_exists="replace", index=False,
                method="multi", chunksize=5000)

    # Verify counts
    with engine.connect() as conn:
        for tbl in ["households","transactions","products"]:
            print(f"{tbl}:", conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar())

if __name__ == "__main__":
    print("1) Downloading blobs…")
    download_blobs()
    print("\n2) Loading into Azure SQL…")
    load_into_sql()
    print("\n✅ Done!")
