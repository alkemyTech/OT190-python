SELECT 
	universities AS university,
	careers AS career,
	inscription_dates AS inscription_date,
	SPLIT_PART(names, '-', 1) first_name,
	SPLIT_PART(names,'-',2) last_name,
	sexo AS gender,
	birth_dates AS birth_date,
	locations AS location,
	emails AS email
FROM 
	public.lat_sociales_cine
where
	universities = 'UNIVERSIDAD-DEL-CINE'
	AND TO_DATE(inscription_dates ,'DD-MM-YY')>= '01-09-2020'
	AND TO_DATE(inscription_dates ,'DD-MM-YY')< '01-02-2021'