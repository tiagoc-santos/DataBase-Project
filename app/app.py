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
        "autocommit": True,  # If True don’t start transactions automatically.
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

@app.route("/c/<clinica>/", methods=("GET",))
def get_specialty(clinica):
    """ Lists all specialties available in a clinic"""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            specialty = cur.execute(
                """
                SELECT DISTINCT m.especialidade
                FROM medico m
                JOIN trabalha t ON m.nif = t.nif
                WHERE t.nome = %(clinica)s;
                """,
                {"clinica": clinica},
            ).fetchall()
    
    if specialty is []:
        return jsonify({"message": "Clinica não encontrada", "status": "error"}), 404
    return jsonify(specialty)

@app.route("/c/<clinica>/<especialidade>/", methods=("GET",))
def get_availability(clinica, especialidade):
    """ Lists 3 available times for an apointment for each doctor"""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            availability = cur.execute(
                """
                WITH medicos_disponiveis AS (
                    SELECT m.nome, m.nif
                    FROM medico m
                    JOIN trabalha t ON m.nif = t.nif
                    WHERE m.especialidade = %(especialidade)s AND t.nome = %(clinica)s
                )                         
                SELECT DISTINCT (m.nome, ha.horario)
                FROM medicos_disponiveis m 
                CROSS JOIN LATERAL (
                    SELECT horario
                    FROM horario_aux ha
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM consulta c
                        WHERE c.nif = m.nif
                        AND ha.horario = c.data + c.hora
                    ) AND 
                    horario > LOCALTIMESTAMP + INTERVAL '1 hour'
                    AND EXISTS (
                        SELECT 1
                        FROM trabalha t
                        WHERE t.nif = m.nif
                        AND EXTRACT(DOW FROM ha.horario) = t.dia_da_semana AND t.nome = %(clinica)s
                    )
                    ORDER BY horario
                    LIMIT 3
                ) AS ha;
                """,
                {"especialidade": especialidade, "clinica": clinica},
            ).fetchall()
    
    #if specialty is []:
    #    return jsonify({"message": "Clinica não encontrada", "status": "error"}), 404
    return jsonify(availability)


def check_args(paciente, doutor, data, hora):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            pacienteExist = cur.execute(
                """
                SELECT *
                FROM paciente p
                WHERE p.ssn = %(paciente)s
                """,
                {"paciente": paciente},
            ).fetchall()
            doutorExist = cur.execute(
                """
                SELECT *
                FROM medico m
                WHERE m.nif = %(doutor)s
                """,
                {"doutor": doutor},
            ).fetchall()
            consulta = cur.execute(
                """
                SELECT *
                FROM consulta c
                WHERE c.nif = %(doutor)s
                AND %(data)s::date + %(hora)s::time = c.data + c.hora
                )
                """,
                {"doutor": doutor, "data": data, "hora": hora},
            ).fetchall()
            passado = cur.execute(
                """
                SELECT '%(data)s::date + %(hora)s::time < NOW();
                """,
                {"data": data, "hora": hora},
            ).fetchone()
    if pacienteExist is None:
        return 0
    if doutorExist is None:
        return 1
    if passado == 'true':
        return 3
    if consulta is not None:
        return 2
    if consulta is None:
        return 4
            

@app.route("/a/<clinica>/registar/", methods=("POST",))
def register_apointment(clinica):
    """ Registers an apointment in a clinic"""
    paciente, doutor, data, hora = None
    paciente = request.args.get("paciente")
    medico = request.args.get("medico")
    data = request.args.get("data")
    hora = request.args.get("hora")

    error = None
    if paciente or medico or data or hora is None:
        error =  "Please enter all required fields"
        return error, 400
    
    argcheck = check_args(paciente, medico, data, hora)

    if argcheck == 0:
        return jsonify({"message": "Please enter a valid SSN", "status": "error"})
    elif argcheck == 1:
        return jsonify({"message": "Please enter a valid doctor NIF", "status": "error"})
    elif argcheck == 2 or argcheck == 3:
        return jsonify({"message": "That time slot is not available", "status": "error"})

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO consulta (ssn, nif, nome, data, hora)
                VALUES (%(paciente)s,%(doutor)s,%(clinica)s,%(data)s,%(hora)s);
                """,
                {"paciente": paciente, "doutor": doutor, "clinica": clinica, "data":data, "hora":hora},
            )
    
    return jsonify({"message": "Apointment reserved succesfully"})

@app.route("/a/<clinica>/cancelar/", methods=("POST",))
def cancel_apointment(clinica):
    """ Cancels an apointment in a clinic"""

    paciente = request.args.get("Paciente SSN")
    doutor = request.args.get("Médico NIF")
    data = request.args.get("Data")
    hora = request.args.get("Hora")

    argcheck = check_args(paciente, doutor, data, hora)

    if argcheck == 0:
        return jsonify({"message": "Please enter a valid SSN", "status": "error"})
    elif argcheck == 1:
        return jsonify({"message": "Please enter a valid doctor NIF", "status": "error"})
    elif argcheck == 3:
        return jsonify({"message": "Enter a valid date", "status": "error"})
    elif argcheck == 4:
        return jsonify({"message": "No apointment reserved on that date", "status": "error"})

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM consulta
                WHERE %(paciente)s = c.ssn, %(doutor)s = c.nif, %(clinica)s = c.nome, %(data)s = c.data, %(hora)s = c.hora;
                """,
                {"paciente": paciente, "doutor": doutor, "clinica": clinica, "data":data, "hora":hora},
            )
    
    return jsonify({"message": "Apointment canceled succesfully"})