import datetime
from app.database import fetch_db_begin_grow

def compute_left_days(conn: object, cam_code: str, location: str):
    # TODO: User more information such as area, species, health, ...

    begin_grow, full_grow_cycle = fetch_db_begin_grow(conn, cam_code, location)

    return round(full_grow_cycle - (datetime.datetime.now(datetime.timezone.utc) - begin_grow).total_seconds()/3600/24, 1)