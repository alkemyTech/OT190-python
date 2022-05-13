SELECT
universidad AS university,
carrera AS career,
fecha_de_inscripcion AS inscription_date,
nombre AS first_name,
sexo AS gender,
fecha_nacimiento AS age,
localidad AS location,
email
FROM "salvador_villa_maria"
WHERE  universidad='UNIVERSIDAD_DEL_SALVADOR' AND
TO_DATE(fecha_de_inscripcion, 'DD-Mon-YY') BETWEEN '2020-09-01'::date AND '2021-02-01'::date;
