USING EXTERNAL FUNCTION h3_line(start BIGINT, en BIGINT)
RETURNS ARRAY(BIGINT)
LAMBDA  '<ARN>'
SELECT h3_line(613499565412188159, 613499457337556991) 