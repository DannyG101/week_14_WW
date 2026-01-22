from fastapi import FastAPI, File, UploadFile
from db import DBConnect
import pandas as pd
import uvicorn
import os
from dotenv import load_dotenv

load_dotenv()


HOST = os.getenv("HOST")


app = FastAPI()



def add_risk_level(df):
    df["risk_level"] = pd.cut(x=df["range_km"], bins=[0, 21, 101, 300, float("inf")], labels=["low", "medium", "high", "extreme"], include_lowest=True)
    return df

def replace_nan(df):
    df["manufacturer"] = df["manufacturer"].fillna("Unknown")
    return df


@app.post("/upload")
def upload_file(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)
    df = add_risk_level(df)
    df = replace_nan(df)
    dict_df = df.to_dict("records")
    db = DBConnect(HOST, "root", "")
    db.connect()
    db.create_db("test5")
    db.create_table("weapons5")
    for weapon in dict_df:
        db.insert_into_table(weapon["weapon_id"], weapon["weapon_name"], weapon["weapon_type"],
                             weapon["range_km"], weapon["weight_kg"],
                             weapon["manufacturer"], weapon["origin_country"], weapon["storage_location"],
                             weapon["year_estimated"], weapon["risk_level"])
    db.close_connection()
    file.file.close()
    return {"status": "success", "inserted_records": len(dict_df)}




