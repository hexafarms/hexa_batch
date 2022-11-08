import datetime
from .database import fetch_db_begin_grow

def compute_left_days(conn: object, cam_code: str, location: str):
    # TODO: User more information such as area, species, health, ...

    begin_grow, full_grow_cycle = fetch_db_begin_grow(conn, cam_code, location)
    return full_grow_cycle - (datetime.now() - begin_grow)