SELECT 
universidad AS university, 
carrerra AS career, 
to_date(fechaiscripccion, 'DD/MM/YYYY') AS inscription_date, 
nombrre AS complete_name, 
sexo AS gender, 
--to_date(nacimiento, 'DD/MM/YYYY') as birthday, 
nacimiento as birthday,
codgoposstal AS postal_code,
NULL as location,
eemail AS email
FROM moron_nacional_pampa
WHERE universidad = 'Universidad de morón' 
AND to_date(fechaiscripccion, 'DD/MM/YYYY') BETWEEN '2020-09-01' AND '2021-02-01'