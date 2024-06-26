DROP TABLE IF EXISTS horario_aux;
CREATE TABLE horario_aux(
	horario TIMESTAMP PRIMARY KEY
);

INSERT INTO horario_aux (horario)
SELECT horario
FROM (
    SELECT generate_series(
        '2024-01-01 08:00:00'::timestamp,
        '2024-12-31 12:30:00'::timestamp,
        '30 minutes'::interval
    ) AS horario
    UNION
    SELECT generate_series(
        '2024-01-01 14:00:00'::timestamp,
        '2024-12-31 18:30:00'::timestamp,
        '30 minutes'::interval
    ) AS horario
) AS horarios
WHERE EXTRACT(HOUR FROM horario) BETWEEN 8 AND 12
   OR EXTRACT(HOUR FROM horario) BETWEEN 14 AND 18;