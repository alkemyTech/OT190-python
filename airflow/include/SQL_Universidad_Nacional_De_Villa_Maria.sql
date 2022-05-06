WITH svm_refined_dates AS(
SELECT
	universidad AS university,
	carrera AS career,
	TO_DATE(fecha_de_inscripcion, 'DD-Mon-YY') AS inscription_date,
	nombre AS first_name,
	'' AS last_name,
	sexo AS gender,
	fecha_nacimiento as age,
	'' AS postal_code,
	localidad AS location,
	email
FROM
	salvador_villa_maria
)
SELECT
	*
FROM
	svm_refined_dates
WHERE
	inscription_date BETWEEN '2020-09-01' AND '2021-02-01'
