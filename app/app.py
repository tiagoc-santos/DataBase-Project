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
    
    if specialty == []:
        return "Clinica não encontrada" , 404
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
    
    if availability == []:
        return "Clinica ou Especialidade não encontrada", 404
    return jsonify(availability)


def check_args(clinica, paciente, doutor, data, hora):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            with conn.transaction():
                pacienteExist = cur.execute(
                    """
                    SELECT *
                    FROM paciente p
                    WHERE p.ssn = %(paciente)s;
                    """,
                    {"paciente": paciente},
                ).fetchall()
                doutorExist = cur.execute(
                    """
                    SELECT *
                    FROM medico m
                    WHERE m.nif = %(doutor)s;
                    """,
                    {"doutor": doutor},
                ).fetchall()
                doctorworks = cur.execute(
                    """
                    SELECT * FROM trabalha t
                    WHERE t.nif = %(doutor)s
                    AND t.nome = %(clinica)s AND EXTRACT(DOW FROM %(data)s::date) = t.dia_da_semana;
                    """,
                    {"doutor": doutor, "clinica": clinica, "data": data},
                ).fetchall()
                consulta = cur.execute(
                    """
                    SELECT *
                    FROM consulta c
                    WHERE c.nif = %(doutor)s
                    AND %(data)s::date + %(hora)s::time = c.data + c.hora;
                    """,
                    {"doutor": doutor, "data": data, "hora": hora},
                ).fetchall()
                passado = cur.execute(
                    """
                    SELECT
                    CASE 
                        WHEN (%(data)s::date + %(hora)s::time) < NOW() THEN 'TRUE'
                        ELSE 'FALSE'
                    END AS result;
                    """,
                    {"data": data, "hora": hora},
                ).fetchone()
    if pacienteExist == []:
        return 0
    if doutorExist == []:
        return 1
    if consulta != []:
        return 2
    if passado[0] == 'TRUE':
       return 3
    if doctorworks == []:
        return 4
    if consulta == []:
        return 5
    
def get_new_id():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            id_consulta = cur.execute(
                """
                SELECT MAX(id) + 1
                FROM consulta;
                """
            ).fetchone()
    id_consulta = id_consulta[0]
    return id_consulta

def check_if_number(number):
    try:
        int(number)
        return True
    except:
        return False

def check_valid_date(data, hora):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            datas = cur.execute(
                """
                SELECT * 
                FROM horario_aux
                WHERE horario = %(data)s::date + %(hora)s::time;
                """,
            {"data": data, "hora": hora}
            ).fetchall()
    if datas == []:
        return False
    else:
        return True


@app.route("/a/<clinica>/registar/", methods=("POST",))
def register_apointment(clinica):
    """ Registers an apointment in a clinic"""

    paciente = request.args.get("paciente")
    doutor = request.args.get("medico")
    data = request.args.get("data")
    hora = request.args.get("hora")
    
    if not check_if_number(paciente) or not check_if_number(doutor):
        return "NIF/SSN must be numbers.", 400

    if not check_valid_date(data, hora):
        return "Enter a valid date/hour", 400
        
    argcheck = check_args(clinica, paciente, doutor, data, hora)

    if argcheck == 0:
        return "Please enter a valid SSN", 400
    elif argcheck == 1:
        return "Please enter a valid doctor NIF", 400
    elif argcheck == 2:
        return "That time slot is not available", 400
    elif argcheck == 3:
        return "Enter a valid date", 400
    elif argcheck == 4:
        return "Doctor does not work at that clinic", 400

    id_consulta = get_new_id()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO consulta (id, ssn, nif, nome, data, hora)
                VALUES (%(id)s, %(paciente)s,%(doutor)s,%(clinica)s,%(data)s,%(hora)s);
                """,
                {"id": id_consulta,"paciente": paciente, "doutor": doutor, "clinica": clinica, "data":data, "hora":hora},
            )
        conn.commit()
    
    return "Apointment reserved succesfully", 200

@app.route("/a/<clinica>/cancelar/", methods=("POST",))
def cancel_apointment(clinica):
    """ Cancels an apointment in a clinic"""

    paciente = request.args.get("paciente")
    doutor = request.args.get("medico")
    data = request.args.get("data")
    hora = request.args.get("hora")
    
    if not check_if_number(paciente) or not check_if_number(doutor):
        return "NIF/SSN must be numbers.", 400

    if not check_valid_date(data, hora):
        return "Enter a valid date/hour", 400

    argcheck = check_args(clinica, paciente, doutor, data, hora)

    if argcheck == 0:
        return "Please enter a valid SSN", 400
    elif argcheck == 1:
        return "Please enter a valid doctor NIF", 400
    elif argcheck == 3:
        return "Enter a valid date", 400
    elif argcheck == 5:
        return "No apointment reserved on that date", 400

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM consulta
                WHERE %(paciente)s = ssn AND %(doutor)s = nif AND %(clinica)s = nome
                AND %(data)s = data AND %(hora)s = hora;
                """,
                {"paciente": paciente, "doutor": doutor, "clinica": clinica, "data":data, "hora":hora},
            )
        conn.commit()
    return "Apointment canceled succesfully", 200
