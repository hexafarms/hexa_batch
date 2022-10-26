from fastapi import FastAPI
import psycopg2
from .database import update_db, fetch_db
import yaml

with open('credential/hexa.yaml', 'r') as stream:
    setup = yaml.safe_load(stream)

app = FastAPI()

@app.get("/")
async def root():
    return {"Guide": "Type {address}/batch/{location}/{cam_code"}

@app.get("/batch/{location}/{cam_code}")
async def create_item(cam_code: str, location: str):
    # Connect to your postgres DB
    conn = psycopg2.connect(
        f"dbname={setup['dbname']} user={setup['user']} host={setup['host']} password={setup['password']}"
        )
    conn.set_session(readonly=False)
    query_i, query_u  = fetch_db(conn, cam_code, location)
    result_i = update_db(conn, query_i)
    result_u = update_db(conn, query_u)
    conn.commit()
        
    return {'return after insert': result_i, 'return after update': result_u}

if __name__ == "__main__":
    conn = psycopg2.connect("dbname={setup['dbname']} user={setup['user']} host={setup['host']} password={setup['password']}")
    conn.set_session(readonly=False)
    query_i, query_u  = fetch_db(conn, 'G8T1-9400-0301-04R7', 'techstars')
    update_db(conn, query_i)
    update_db(conn, query_u)
    conn.commit()
