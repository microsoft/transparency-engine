import os

from sqlalchemy import MetaData, Table, select

from api_backend.report.util.parsers import parse_value
from api_backend.util.db_engine import get_engine


def get_report_score_from_db(entity_id):
    table_name = os.getenv("NETWORK_SCORING_TABLE", "")
    engine = get_engine()

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    query = select(table).where(table.c.EntityID == entity_id)

    conn = engine.connect()
    result_proxy = conn.execute(query)
    result = result_proxy.fetchone()
    column_names = [col for col in result_proxy.keys()]
    conn.close()

    if "final_network_score_max_scaled" in column_names:
        i = column_names.index("final_network_score_max_scaled")
        score = result[i]
        return {"score": score}
    return None
