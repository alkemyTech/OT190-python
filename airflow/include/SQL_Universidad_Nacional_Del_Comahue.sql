SELECT 
universidad AS university, 
carrera AS career, 
fecha_de_inscripcion AS inscription_date, 
name AS first_name,
sexo AS gender,
fecha_nacimiento AS age,
codigo_postal AS postal_code,
direccion AS location,
correo_electronico AS email
FROM "flores_comahue" 
WHERE universidad='UNIV. NACIONAL DEL COMAHUE'
AND TO_DATE(fecha_de_inscripcion, 'YYYY/MM/DD') BETWEEN '2020-09-01'::date AND '2021-02-01'::date;







