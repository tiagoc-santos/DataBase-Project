import os
from logging.config import dictConfig

import psycopg
from flask import Flask, jsonify, request
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://postgres:postgres@postgres/saude")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={
        "autocommit": False,  # If True don’t start transactions automatically.
        "row_factory": namedtuple_row,
    },
    min_size=4,
    max_size=10,
    open=True,
    # check=ConnectionPool.check_connection,
    name="postgres_pool",
    timeout=5,
)

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

app = Flask(__name__)
app.config.from_prefixed_env()
log = app.logger

@app.route("/", methods=("GET",))
def get_clinics():
    """ Lists both adress and name from all available clinics"""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            clinicas = cur.execute(
                """
                SELECT nome, morada 
                FROM clinica;
                """,
                {}
            ).fetchall()
            
    return jsonify(clinicas)