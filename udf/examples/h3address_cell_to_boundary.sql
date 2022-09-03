USING EXTERNAL FUNCTION cell_to_boundary(h3 VARCHAR, sep VARCHAR)
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
SELECT cell_to_boundary(h3_cell, ',') AS coord
FROM(
USING EXTERNAL FUNCTION lat_lng_to_cell_address(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS VARCHAR
LAMBDA '<ARN>'
  SELECT lat_lng_to_cell_address(latitude, longitude, 10) AS h3_cell
  FROM earthquakes
  WHERE latitude BETWEEN 18 AND 70)