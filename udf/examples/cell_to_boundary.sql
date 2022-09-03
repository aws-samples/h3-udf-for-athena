USING EXTERNAL FUNCTION cell_to_boundary(h3 BIGINT, SEP VARCHAR)
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
WITH dataset AS (
    SELECT h3_cell, cell_to_boundary(h3_cell,',') AS coords
        FROM(
        USING EXTERNAL FUNCTION lat_lng_to_cell(lat DOUBLE, lng DOUBLE, res INTEGER)
        RETURNS BIGINT
        LAMBDA '<ARN>'
        SELECT lat_lng_to_cell(latitude, longitude, 10) AS h3_cell
        FROM demo.earthquakes
        WHERE latitude BETWEEN 18 AND 70) )
SELECT h3_cell, p as point from dataset CROSS JOIN  unnest(coords) AS t(p)