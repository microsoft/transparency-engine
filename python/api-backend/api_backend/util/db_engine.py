import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .load_env import load_env

load_env()


def get_engine() -> Engine:
    """
    Get a SQLAlchemy engine for the database.

    Returns:
        A SQLAlchemy Engine object for connecting to the database.
    """
    server = os.environ["SQL_ENDPOINT"]
    database = os.environ["DB_NAME"]
    username = os.environ["SQL_USERNAME"]
    password = os.environ["SQL_PASSWORD"]
    # or possibly 17 or 18 or {SQL Server Native Client 11.0}
    driver = "{ODBC Driver 17 for SQL Server}"

    odbc_connect = f"DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password};Authentication=ActiveDirectoryServicePrincipal;"

    url = "mssql+pyodbc:///?odbc_connect={}".format(odbc_connect)

    engine = create_engine(url, echo=False)

    return engine
