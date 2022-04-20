-- Obtengo los datos de las personas anotadas en la universidad Nacional de Jujuy
-- entre las fechas 01/9/2020 al 01/02/2021. Las ordeno ascendentemente.

SELECT 
    university,
    career,
    inscription_date,
    nombre,
    sexo,
    birth_date,
    location,
    email
FROM 
    jujuy_utn
WHERE 
    university = 'universidad nacional de jujuy' 
    and (date(inscription_date) BETWEEN '2020/09/01' and  '2021/02/01' )
    order by date(inscription_date) asc;