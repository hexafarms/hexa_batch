import pandas as pd

def fetch_db(conn: object, code: str, location: str, valid: bool):
    """Fetch data from db"""
    # Open a cursor to perform database operations
    cur = conn.cursor()
    # Execute a query
    cur.execute(f"SELECT * FROM top_view WHERE batch_id is NULL AND location = \
                (SELECT location_id from locations WHERE location='{location}') AND \
                    file_name LIKE '%{code}%';")
    colnames = [desc[0] for desc in cur.description]
    # Retrieve query results
    result = cur.fetchall()

    """make sql query based on cam_code and time and location"""
    df = pd.DataFrame(result, columns=colnames)
    df['unix_t'] = df['file_name'].apply(lambda x: x.split('-')[-1])
    df.sort_values(by='unix_t', ascending=False, inplace=True)

    """Insert Query"""
    begin_time, end_time = df.unix_t.min(), df.unix_t.max()
    query_i = f"INSERT INTO batchs(code, begin_time, end_time, location) VALUES \
        ('{location}_{code}', {begin_time}, {end_time}, (SELECT location_id from locations WHERE location='{location}') );"
    
    """Upate Query"""
    ids = tuple(df['id'].to_list()[:-1]) # Leave the last image because of the unavailable error at home assistant
    
    if valid is True:
        """
        If the batch is unnucessary to be updated. 
        For example, time after harvest and before transplant
        """
        query_u = f"UPDATE top_view SET batch_id = (SELECT batch_id FROM batchs WHERE begin_time={begin_time} AND end_time={end_time} AND code LIKE '%{code}%') \
            WHERE id in {ids};"
    else:
        query_u = f"UPDATE top_view SET batch_id = 0 WHERE id in {ids};"

    return query_i, query_u


def update_db(conn: object, query: str):
    """Update batch data to db"""
    with conn.cursor() as curs:
        curs.execute(query)
    return curs.statusmessage


def fetch_db_grow_id(conn, cam_code, location):
    """Fetch the most recent grow id from db """

    # Open a cursor to perform database operations
    cur = conn.cursor()
    # Execute a query
    query_grow_id = f"SELECT grow_id FROM predict_harvest \
    WHERE begin_grow = (SELECT max(begin_grow) FROM predict_harvest WHERE \
    location_id = (SELECT location_id FROM locations WHERE location = '{location}') AND \
    cam_code = '{cam_code}');"
    
    cur.execute(query_grow_id)

    # Retrieve query results
    result = cur.fetchall()

    grow_id = result[0][0]
    return grow_id  