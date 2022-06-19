USING EXTERNAL FUNCTION h3_to_geo_boundary(h3 VARCHAR, sep VARCHAR)
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
SELECT h3_to_geo_boundary(h3_cell, ',') AS coord
FROM(
USING EXTERNAL FUNCTION geo_to_h3_address(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS VARCHAR
LAMBDA '<ARN'
  SELECT geo_to_h3_address(latitude, longitude, 10) AS h3_cell
  FROM earthquakes
  WHERE latitude BETWEEN 18 AND 70)