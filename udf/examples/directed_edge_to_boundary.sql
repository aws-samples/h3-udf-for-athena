WITH dataset AS(
USING EXTERNAL FUNCTION get_pentagons(res INT) 
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
SELECT get_pentagons(10) as pentagons)
SELECT pentagon FROM dataset CROSS JOIN unnest(pentagons) AS t(pentagon) 
