SELECT
	universidad AS university,
	carrera AS career,
	TO_DATE(fecha_de_inscripcion, 'YYYY-MM-DD') AS inscription_date,
	name AS first_name,
	'' last_name,
	sexo AS gender,
	fecha_nacimiento as age,
	codigo_postal AS postal_code,
	'' AS location,
	correo_electronico AS email 	
FROM
	flores_comahue
WHERE
	fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01'
