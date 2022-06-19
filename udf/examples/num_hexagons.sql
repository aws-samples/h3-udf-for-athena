
USING EXTERNAL FUNCTION num_hexagons(res INT) 
RETURNS BIGINT
LAMBDA '<ARN>'
SELECT num_hexagons(1), num_hexagons(10);