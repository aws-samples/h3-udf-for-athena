USING EXTERNAL FUNCTION h3_distance(start VARCHAR, en VARCHAR)
RETURNS INTEGER
LAMBDA  '<ARN>'
SELECT h3_distance('883969ab23fffff', '8839681887fffff') 
