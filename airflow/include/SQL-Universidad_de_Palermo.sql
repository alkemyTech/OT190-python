-- Obtengo los datos de las personas anotadas en la universidad de Palermo
-- entre las fechas 01/9/2020 al 01/02/2021. Las ordeno ascendentemente.

SELECT 
    universidad,
    careers,
    fecha_de_inscripcion,
    names,
    sexo,
    birth_dates,
    correos_electronicos,
    codigo_postal
FROM 
    palermo_tres_de_febrero 
WHERE 
    universidad = '_universidad_de_palermo' 
    and (date(fecha_de_inscripcion) BETWEEN '01/Sep/20' and  '01/Feb/21' )
    order by date(fecha_de_inscripcion) asc;
