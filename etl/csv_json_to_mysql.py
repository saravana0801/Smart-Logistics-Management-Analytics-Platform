import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pymysql
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATASET_PATH = ROOT / "project_dataset"
SQL_PATH = ROOT / "sql" / "schema.sql"

def load_env():
    load_dotenv(ROOT / ".env")
    return{
        'host' : os.getenv('DB_HOST'),
        'port' : int(os.getenv('DB_PORT')),
        'name' : os.getenv('DB_NAME'),    
        'user' : os.getenv('DB_USER'),
        'password' : os.getenv('DB_PASSWORD'),
    }

def make_engine(confg):
    url = f"mysql+pymysql://{confg['user']}:{confg['password']}@{confg['host']}:{confg['port']}/{confg['name']}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)

def run_schema(engine):
    schema_path = SQL_PATH
    sql = schema_path.read_text()
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        for query in sql.split(';'):
            query = query.strip()
            if not query:
                continue
            cursor.execute(query)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
        
def read_project_dataset():
    dfs = {}

    path = DATASET_PATH/'courier_staff.csv'
    if path.exists():
        dfs['courier_staff'] = pd.read_csv(path)

    path = DATASET_PATH / 'routes.csv'
    if path.exists():
        dfs['routes'] = pd.read_csv(path)

    path = DATASET_PATH / 'warehouses.json'
    if path.exists():
        dfs['warehouses'] = pd.read_json(path)

    path = DATASET_PATH/'shipments.json'
    if path.exists():
        dfs['shipments'] = pd.read_json(path)

    path = DATASET_PATH / 'costs.csv'
    if path.exists():
        dfs['costs'] = pd.read_csv(path)

    path_csv = DATASET_PATH / 'shipment_tracking.csv'
    
    if path_csv.exists():
        dfs['shipment_tracking'] = pd.read_csv(path_csv)
    
    return dfs

def preprocessing(dfs):

    if 'courier_staff' in dfs:
        df = dfs['courier_staff']
        if 'rating' in df.columns:
            df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0.0)
        dfs['courier_staff'] = df

    if 'shipments' in dfs:
        df = dfs['shipments']
        for col in ['order_date', 'delivery_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        if 'weight' in df.columns:
            df['weight']= pd.to_numeric(df['weight'],errors='coerce').fillna(0.0)        
        dfs['shipments'] = df

    if 'routes' in dfs:
        df = dfs['routes']
        for col in ['distance_km', 'avg_time_hours']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        dfs['routes'] = df

    # costs: numeric
    if 'costs' in dfs:
        df = dfs['costs']
        for col in ['fuel_cost', 'labor_cost', 'misc_cost']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        dfs['costs'] = df

    # shipment_tracking: timestamp coercion
    if 'shipment_tracking' in dfs:
        df = dfs['shipment_tracking']
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        dfs['shipment_tracking'] = df

    return dfs

def load_tables(engine,dfs):

    load_order = ['courier_staff', 'routes', 'warehouses', 'shipments', 'costs', 'shipment_tracking']
    primary_keys = {
        'courier_staff': 'courier_id',
        'shipments': 'shipment_id',
        'routes': 'route_id',
        'warehouses': 'warehouse_id',
        'costs': 'shipment_id',
        'shipment_tracking': 'tracking_id'
    }

    def existing_keys(table, key_col):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT {key_col} FROM {table}"))
                return {row[0] for row in result}
        except Exception:
            return set()
        
    for table in load_order:
        if table not in dfs:
            continue

        df = dfs[table]
        if df.empty:
            continue

        pk=primary_keys.get(table)

        if pk and pk in df.columns:
            df = df.drop_duplicates(subset=[pk], keep='first')

            existing = existing_keys(table, pk)
            if existing:
                before = len(df)
                df = df[~df[pk].isin(existing)]
                removed = before - len(df)
                if removed:
                    print(f"Filtered {table}: removed {removed} rows already in DB.")

            if df.empty:
                print(f"All rows for {table} already exist in DB; skipping.")
                continue

        print(f"Loading {table} ({len(df)} rows)...")
        # Try a bulk insert; on failure, retry in smaller chunks and report chunk errors
        try:
            df.to_sql(table, con=engine, if_exists='append', index=False, method='multi', chunksize=200)
        except Exception as e:
            print(f"Insert failed for {table}: {e}. Retrying in smaller chunks.")
            chunk_size = 100
            for i in range(0, len(df), chunk_size):
                sub = df.iloc[i:i+chunk_size]
                try:
                    sub.to_sql(table, con=engine, if_exists='append', index=False, method='multi')
                except Exception as e2:
                    print(f"Chunk insert failed for {table} rows {i}-{i+chunk_size}: {e2}")

        print(f"Loaded {table}.") 

     
def main():    
    print("Loading environment variables")
    confg = load_env()
    print("Environment variables loaded")

    print("Creating database engine")
    engine = make_engine(confg)
    print("Database engine created")

    print("Running schema")
    run_schema(engine)
    print("Schema executed successfully")

    print("Reading project dataset")
    dfs = read_project_dataset()
    print("Project dataset read successfully")  

    print("Data preprocessing started")
    dfs = preprocessing(dfs)
    print("Data preprocessing completed")

    print("Starting table loading")
    load_tables(engine,dfs)
    print("Table loading completed")

if __name__ == "__main__":
    main()