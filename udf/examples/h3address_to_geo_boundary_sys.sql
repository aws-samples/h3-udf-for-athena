USING EXTERNAL FUNCTION h3_to_geo_boundary_sys(h3 VARCHAR, cord VARCHAR)
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
SELECT zip(h3_to_geo_boundary_sys(h3_cell, 'lat'), 
           h3_to_geo_boundary_sys(h3_cell, 'lng') )AS coord
FROM(
USING EXTERNAL FUNCTION geo_to_h3_address(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS VARCHAR
LAMBDA '<ARN>'
  SELECT geo_to_h3_address(latitude, longitude, 10) AS h3_cell
  FROM earthquakes
  WHERE latitude BETWEEN 18 AND 70)