SET datestyle = "ISO, DMY";

SELECT 
	universities AS university,
	careers AS career, 
	inscription_dates AS inscription_date,
	names AS last_name,
	sexo AS gender, 
	birth_dates AS age,
	locations AS location,
	emails AS email

FROM lat_sociales_cine 
WHERE universities ='-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES' 
AND cast(inscription_dates as DATE) BETWEEN '2020-09-01' AND '2021-02-01';