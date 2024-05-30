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
        "autocommit": True,
        "row_factory": namedtuple_row,
    },
    min_size=4,
    max_size=10,
    open=True,
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
    """Lists both adress and name from all available clinics"""
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
        return jsonify({"message": "Clinica não encontrada"}), 404
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
        return jsonify({"message": "Clinica ou Especialidade não encontrada"}), 404
    return jsonify(availability)


def check_paciente(paciente):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            pacienteExist = cur.execute(
                """
                SELECT *
                FROM paciente p
                WHERE p.ssn = %(paciente)s;
                """,
                {"paciente": paciente},
            ).fetchall()
    if pacienteExist == []:
        return False
    return True

def check_medico(medico):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            medicoExist = cur.execute(
                """
                SELECT *
                FROM medico m
                WHERE m.nif = %(medico)s;
                """,
                {"medico": medico},
            ).fetchall()
    if medicoExist == []:
        return False
    return True

def check_medico_clinica(medico, clinica, data):
     with pool.connection() as conn:
        with conn.cursor() as cur:
            doctorworks = cur.execute(
                """
                SELECT * FROM trabalha t
                WHERE t.nif = %(medico)s
                AND t.nome = %(clinica)s AND EXTRACT(DOW FROM %(data)s::date) = t.dia_da_semana;
                """,
                {"medico": medico, "clinica": clinica, "data": data},
            ).fetchall()
        if doctorworks == []:
            return False
        return True


def check_consulta(paciente, medico, data, hora):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            with conn.transaction():
                consulta_medico = cur.execute(
                    """
                    SELECT *
                    FROM consulta c
                    WHERE c.nif = %(medico)s
                    AND %(data)s::date + %(hora)s::time = c.data + c.hora;
                    """,
                    {"medico": medico, "data": data, "hora": hora},
                ).fetchall()
                consulta_paciente = cur.execute(
                    """
                    SELECT *
                    FROM consulta c
                    WHERE c.ssn = %(paciente)s
                    AND %(data)s::date + %(hora)s::time = c.data + c.hora;
                    """,
                    {"paciente": paciente, "data": data, "hora": hora},
                ).fetchall()
    if consulta_medico != [] or consulta_paciente != []:
        return False
    return True

def check_data_passado(data, hora):
    with pool.connection() as conn:
        with conn.cursor() as cur:
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
    if passado[0] == 'TRUE':
       return False
    return True


def check_medico_paciente(paciente, medico):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            medico_paciente = cur.execute(
                """
                SELECT nif
                FROM paciente
                WHERE ssn = %(paciente)s;
                """,
                {"medico": medico, "paciente": paciente},
            ).fetchone()
    if medico_paciente[0] == medico:
        return False
    return True
      
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
def register_appointment(clinica):
    """ Registers an appointment in a clinic"""

    paciente = request.args.get("paciente")
    medico = request.args.get("medico")
    data = request.args.get("data")
    hora = request.args.get("hora")
    
    if not check_if_number(paciente) or not check_if_number(medico):
        return jsonify({"message": "NIF/SSN têm de ser números inteiros."}), 400

    if not check_medico_paciente(paciente, medico):
        return jsonify({"message": "Médico não se pode consultar a si próprio."}), 400
    
    if not check_valid_date(data, hora):
        return jsonify({"message": "Insira uma hora/data válida."}), 400
        
    if not check_paciente(paciente):
        return jsonify({"message":"Insira um SSN válido."}), 400
    
    if not check_medico(medico):
        return jsonify({"message":"Insira um NIF válido."}), 400
    
    if not check_consulta(paciente, medico, data, hora):
        return jsonify({"message": "Este horário não está disponível."}), 400
    
    if not check_data_passado(data, hora):
        return jsonify({"message": "Insira uma data válida."}), 400
    
    if not check_medico_clinica(medico, clinica, data):
        return jsonify({"message": "Dr/Dra não trabalha nesta clínica neste dia."}), 400

    id_consulta = get_new_id()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO consulta (id, ssn, nif, nome, data, hora)
                VALUES (%(id)s, %(paciente)s,%(medico)s,%(clinica)s,%(data)s,%(hora)s);
                """,
                {"id": id_consulta,"paciente": paciente, "medico": medico, "clinica": clinica, "data":data, "hora":hora},
            )
        conn.commit()
    
    return jsonify({"message": "Consulta registada com sucesso"}), 200

@app.route("/a/<clinica>/cancelar/", methods=("POST",))
def cancel_appointment(clinica):
    """ Cancels an Appointment in a clinic"""

    paciente = request.args.get("paciente")
    medico = request.args.get("medico")
    data = request.args.get("data")
    hora = request.args.get("hora")
    
    if not check_if_number(paciente) or not check_if_number(medico):
        return jsonify({"message": "NIF/SSN têm de ser números inteiros."}), 400
    
    if not check_medico_paciente(paciente, medico):
        return jsonify({"message": "Médico não se pode consultar a si próprio."}), 400
    
    if not check_valid_date(data, hora):
        return jsonify({"message": "Insira uma hora/data válida."}), 400

    if not check_paciente(paciente):
        return jsonify({"message":"Please enter a valid SSN."}), 400
    
    if not check_medico(medico):
        return jsonify({"message":"Insira um NIF válido."}), 400
    
    if not check_data_passado(data, hora):
        return jsonify({"message": "Insira uma data válida."}), 400
    
    if not check_medico_clinica(medico, clinica, data):
        return jsonify({"message": "Dr/Dra não trabalha nesta clínica neste dia."}), 400
    
    if check_consulta(paciente, medico, data, hora):
        return jsonify({"message": "Não existe uma consulta marcada para este horário."}), 400

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM consulta
                WHERE %(paciente)s = ssn AND %(medico)s = nif AND %(clinica)s = nome
                AND %(data)s = data AND %(hora)s = hora;
                """,
                {"paciente": paciente, "medico": medico, "clinica": clinica, "data":data, "hora":hora},
            )
        conn.commit()
    return jsonify({"message": "Consulta registada com sucesso"}), 200
