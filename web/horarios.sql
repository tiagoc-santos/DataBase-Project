DROP TABLE IF EXISTS horario_aux;
CREATE TABLE horario_aux(
	horario TIMESTAMP PRIMARY KEY
);

INSERT INTO horarios_aux (horario)
SELECT horario
FROM (
    SELECT generate_series(
        '2024-01-01 08:00:00'::timestamp,
        '2024-12-31 13:00:00'::timestamp,
        '30 minutes'::interval
    ) AS horario
    UNION
    SELECT generate_series(
        '2024-01-01 14:00:00'::timestamp,
        '2024-12-31 19:00:00'::timestamp,
        '30 minutes'::interval
    ) AS horario
) AS horarios
WHERE EXTRACT(HOUR FROM horario) BETWEEN 8 AND 12
   OR EXTRACT(HOUR FROM horario) BETWEEN 14 AND 18;