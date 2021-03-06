-- Query for Universidad Tecnológica Nacional
SELECT
  university,
  career,
  inscription_date,
  nombre as name,
  sexo as gender,
  birth_date,
  location,
  email
FROM
  jujuy_utn
WHERE
  university = 'universidad tecnológica nacional'
  AND TO_DATE(inscription_date, 'YYYY/MM/DD') >= '2020/09/01'
  AND TO_DATE(inscription_date, 'YYYY/MM/DD') <= '2021/02/01';
  