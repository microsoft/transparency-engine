#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#
import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    """
    Get a SQLAlchemy engine for the database.

    Returns:
        A SQLAlchemy Engine object for connecting to the database.
    """

    server = os.getenv("SQL_ENDPOINT", "")
    database = os.getenv("DB_NAME", "")
    username = os.getenv("SQL_USERNAME", "")
    password = os.getenv("SQL_PASSWORD", "")

    driver = "{ODBC Driver 17 for SQL Server}"

    odbc_connect = f"DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password};Authentication=ActiveDirectoryServicePrincipal;"

    url = "mssql+pyodbc:///?odbc_connect={}".format(odbc_connect)

    engine = create_engine(url, echo=False)

    return engine
