SELECT 
univiersities AS university, 
carrera AS career, 
to_date(inscription_dates, 'DD/Mon/YY') AS inscription_date, 
names AS complete_name, 
sexo AS gender, 
--to_date(fechas_nacimiento, 'YY/Mon/DD') as birthday, 
fechas_nacimiento as birthday, 
NULL as postal_code,
localidad as location, 
email AS email
FROM rio_cuarto_interamericana
WHERE univiersities = 'Universidad-nacional-de-río-cuarto'
AND to_date(inscription_dates, 'DD/Mon/YY') BETWEEN '2020-09-01' AND '2021-02-01'