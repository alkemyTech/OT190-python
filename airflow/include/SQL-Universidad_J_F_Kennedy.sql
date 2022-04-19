SET datestyle = "ISO, DMY";

SELECT 
	universidades AS university,
	carreras AS career, 
	fechas_de_inscripcion AS inscription_date,
	nombres AS last_name,
	sexo AS gender, 
	fechas_nacimiento AS age,
	codigos_postales AS postal_code,
	emails AS email

FROM uba_kenedy
WHERE universidades ='universidad-j.-f.-kennedy' 
AND cast(fechas_de_inscripcion as DATE) BETWEEN '2020-09-01' AND '2021-02-01';