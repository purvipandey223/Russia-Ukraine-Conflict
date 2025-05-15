import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os


load_dotenv()

KOBO_USERNAME=os.getenv("KOBO_USERNAME")
KOBO_PASSWORD=os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL="https://kf.kobotoolbox.org/api/v2/assets/aZXWsZGZhqLn3xMaXUDff7/export-settings/esLzUHFGpDxBvpUMp7hofRS/data.csv"

PG_HOST=os.getenv("PG_HOST")
PG_DATABASE=os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD=os.getenv("PG_PASSWORD")
PG_PORT= os.getenv("PG_PORT")


schema_name="war"
table_name="russia_ukraine_conflict"

print("Fetching data from KOBOToolbox...")
response=requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

if response.status_code== 200:
    print("👍Data fetched succesfully...")

    csv_data=io.StringIO(response.text)
    df= pd.read_csv(csv_data,sep=";",on_bad_lines="skip")

    print("Processing data...")
    df.columns = [col.strip().replace(" ","_").replace("&","and").replace("-","_") for col in df.columns]

    df["Total_Soldier_Casualties"] = df[["Casualties", "Injured", "Captured"]].sum(axis=1)

    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')

    print("Uploading data to PostgreSQL...")

    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )

    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")
    cur.execute(f"""
                CREATE TABLE {schema_name}.{table_name}(
                    id SERIAL PRIMARY KEY,
                    "start" TIMESTAMP,
                    "end" TIMESTAMP,
                    "Date" DATE,
                    Country TEXT,
                    event TEXT,
                    oblast TEXT,
                    casualties INT,
                    injured INT,
                    captured INT,
                    civilian_casualties INT,
                    new_recruits INT,
                    combat_intensity FLOAT,
                    territory_status TEXT,
                    percentage_occupied FLOAT,
                    area_occupied FLOAT,
                    total_casualties INT
                );
             """)
    

    insert_query = f"""
        INSERT INTO {schema_name}.{table_name} (
            "start", "end", "Date", country, event, oblast, casualties, injured,
            captured, civilian_casualties, new_recruits, combat_intensity, territory_status,
            percentage_occupied, area_occupied
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row.get("start"),
            row.get("end"),
            row.get("Date"),
            row.get("Country"),
            row.get("Event"),
            row.get("Oblast"),
            row.get("Casualties", 0),
            row.get("Injured", 0),
            row.get("Captured", 0),
            row.get("Civilian_Casualties", 0),
            row.get("New_Recruits", 0),
            row.get("Combat_Intensity", 0),
            row.get("Territory_Status"),
            row.get("Percentage_Occupied", 0),
            row.get("Area_Occupied", 0),
        ))
        
        conn.commit()
        #cur.close()
        #conn.close()

        print("👍Data succesfully loaded to PostgreSQL!")

else: 
    print(f"😢Failed to tech data. Status code: {response.status_code}")