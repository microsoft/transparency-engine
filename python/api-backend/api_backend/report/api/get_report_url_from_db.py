import os

from sqlalchemy import MetaData, Table, select

from api_backend.util.db_engine import get_engine


def get_report_url_from_db(entity_id):
    table_name = os.getenv("REPORT_URL_TABLE", "")
    engine = get_engine()

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    query = select(table).where(table.c.EntityID == entity_id)

    conn = engine.connect()
    result = conn.execute(query).fetchone()
    conn.close()

    response = None

    if result is not None:
        response = {
            "EntityID": result[0],
            "ReportLink": result[1],
        }

    return response
