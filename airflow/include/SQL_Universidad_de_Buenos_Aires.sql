SELECT 
	universidades AS university,
	carreras AS career,
	fechas_de_inscripcion AS inscription_date,
	SPLIT_PART(nombres, '-', 1) first_name,
	SPLIT_PART(nombres,'-',2) last_name,
	sexo AS gender,
	fechas_nacimiento AS birth_date,
	codigos_postales AS postal_code,
	direcciones AS location,
	emails AS email
FROM 
	public.uba_kenedy
where
	universidades = 'universidad-de-buenos-aires'
	AND TO_DATE(fechas_de_inscripcion,'DD-Mon-YY')>= '01-Sep-2020'
	AND TO_DATE(fechas_de_inscripcion,'DD-Mon-YY')< '01-Feb-2021'
