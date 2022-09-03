USING EXTERNAL FUNCTION cell_to_boundary_sys(h3 VARCHAR, cord VARCHAR)
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
SELECT zip(cell_to_boundary_sys(h3_cell, 'lat'), 
           cell_to_boundary_sys(h3_cell, 'lng') )AS coord
FROM(
USING EXTERNAL FUNCTION lat_lng_to_cell_address(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS VARCHAR
LAMBDA '<ARN>'
  SELECT lat_lng_to_cell_address(latitude, longitude, 10) AS h3_cell
  FROM earthquakes
  WHERE latitude BETWEEN 18 AND 70)