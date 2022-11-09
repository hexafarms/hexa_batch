from fastapi import FastAPI
import psycopg2
from database import update_db, fetch_db_query, fetch_db_grow_id
from compute import compute_left_days
import yaml

with open('credential/hexa.yaml', 'r') as stream:
    setup = yaml.safe_load(stream)

app = FastAPI()

@app.get("/")
async def root():
    return {"Guide": "Type {address}/{batch_done|batch_start}/{location}/{cam_code}"}


@app.get("/predict_harvest/{location}/{cam_code}")
async def predict_harvest(cam_code: str, location: str, begin_grow_state:float | None = 4.0, full_grow_cycle:float | None = 28.0):
    # Update predict_harvest
    # Connect to your postgres DB
    conn = psycopg2.connect(
        f"dbname={setup['dbname']} user={setup['user']} host={setup['host']} password={setup['password']}"
        )
    conn.set_session(autocommit=True, readonly=False)

    #TODO: compute full_grow_cycle based on the species
    #TODO: compute begin_grow_state based on the area
    
    left_days = compute_left_days(conn, cam_code, location)

    grow_id = fetch_db_grow_id(conn, cam_code, location)

    # Update left_days of the most recent harvest prediction.
    query_predict_harvest = f"UPDATE predict_harvest SET full_grow_cycle={full_grow_cycle}, begin_grow_state={begin_grow_state}, \
left_days={left_days} WHERE grow_id={grow_id};"

    result_predict_harvest = update_db(conn, query_predict_harvest)
    return {'state': 'successful', 'return after update_harvest': result_predict_harvest}


@app.get("/batch_done/{location}/{cam_code}")
async def create_item(cam_code: str, location: str):
    # Connect to your postgres DB
    conn = psycopg2.connect(
        f"dbname={setup['dbname']} user={setup['user']} host={setup['host']} password={setup['password']}"
        )
    conn.set_session(readonly=False)

    # Allocate batch
    query_i, query_u  = fetch_db_query(conn, cam_code, location, valid=True)
    result_i = update_db(conn, query_i)
    result_u = update_db(conn, query_u)
    conn.commit()
        
    return {'state': 'successful', 'return after insert': result_i, 'return after update': result_u}

# additional parameters: full grow cycle & begin grow state
@app.get("/batch_start/{location}/{cam_code}")
async def create_item(cam_code: str, location: str):
    # Connect to your postgres DB
    conn = psycopg2.connect(
        f"dbname={setup['dbname']} user={setup['user']} host={setup['host']} password={setup['password']}"
        )
    conn.set_session(readonly=False)

    # Allocate batch (Ignore data after harvest and before transplat.)
    _, query_u  = fetch_db_query(conn, cam_code, location, valid=False)
    result_batch = update_db(conn, query_u)
    conn.commit()

    query_predict_harvest = f"INSERT INTO predict_harvest(location_id, cam_code) \
VALUES ((SELECT location_id FROM locations WHERE location = '{location}'),'{cam_code}');"
    result_predict_harvest = update_db(conn, query_predict_harvest)
        
    return {'state': 'successful', 'return after update_batch': result_batch, 'return after update_harvest': result_predict_harvest}


if __name__ == "__main__":
    with open('credential/hexa.yaml', 'r') as stream:
        setup = yaml.safe_load(stream)

    conn = psycopg2.connect(f"dbname={setup['dbname']} user={setup['user']} host={setup['host']} password={setup['password']}")
    conn.set_session(readonly=False)
    predict_harvest(conn, 'test_code', 'techstars')
    conn.commit()
