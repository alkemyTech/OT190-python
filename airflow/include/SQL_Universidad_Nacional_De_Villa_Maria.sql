WITH svm_refined_dates AS(
SELECT
	universidad AS university,
	carrera AS career,
	TO_DATE(fecha_de_inscripcion, 'DD-Mon-YY') AS inscription_date,
	nombre AS first_name,
	'' AS last_name,
	sexo AS gender,
	ABS(extract(year FROM AGE(TO_DATE(fecha_nacimiento, 'DD-Mon-YY'), NOW()))) AS age,
	SPLIT_PART(direccion, '_', 1) AS postal_code,
	localidad AS LOCATION,
	email
FROM
	salvador_villa_maria
)
SELECT
	*
FROM
	svm_refined_dates
WHERE
	inscription_date BETWEEN '2020-09-01' AND '2021-02-01';