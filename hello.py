from sqlalchemy import create_engine, text

engine = create_engine(
    "mssql+pyodbc://"
    "sqladmin%40retailsqlsrv29:YourStrongP%40ss%21"
    "@retailsqlsrv29.database.windows.net:1433/RetailDB"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

with engine.connect() as conn:
    print(conn.execute(text("SELECT 1")).scalar())
