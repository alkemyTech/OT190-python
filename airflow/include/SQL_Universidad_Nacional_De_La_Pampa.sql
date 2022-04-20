SELECT 
	universidad AS university, 
	carrerra AS career,
	fechaiscripccion AS inscription_date,
	nombrre AS first_name,
	'' AS last_name,
	sexo AS gender,
	nacimiento AS age,
	codgoposstal AS postal_code,
	direccion AS "location",
	eemail AS email
FROM moron_nacional_pampa
WHERE universidad = 'Universidad nacional de la pampa'
AND TO_DATE(fechaiscripccion, 'DD/MM/YYYY') BETWEEN '2020-09-01'::date AND '2021-02-01'::date;
