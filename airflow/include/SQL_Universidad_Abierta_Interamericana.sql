SELECT 
	univiersities AS university, 
	carrera AS career,
	inscription_dates AS inscription_date,
	"names" AS first_name,
	'' AS last_name,
	sexo AS gender,
	fechas_nacimiento AS age,
	localidad AS postal_code,
	direcciones AS "location",
	email
FROM public.rio_cuarto_interamericana
WHERE univiersities = '-universidad-abierta-interamericana'
AND TO_DATE(inscription_dates, 'YY/Mon/DD') BETWEEN '2020-09-01'::date AND '2021-02-01'::date;
