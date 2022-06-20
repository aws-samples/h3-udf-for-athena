USING EXTERNAL FUNCTION h3_to_geo_boundary_wkt(h3 BIGINT)
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
SELECT h3_to_geo_boundary_wkt(h3_cell) AS coord
FROM(
USING EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS BIGINT
LAMBDA '<ARN>'
  SELECT geo_to_h3(latitude, longitude, 10) AS h3_cell
  FROM earthquakes
  WHERE latitude BETWEEN 18 AND 70)