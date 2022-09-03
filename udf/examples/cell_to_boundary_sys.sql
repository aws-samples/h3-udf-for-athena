USING EXTERNAL FUNCTION cell_to_boundary_sys(h3 BIGINT, coordsys VARCHAR)
RETURNS ARRAY(DOUBLE)
LAMBDA '<ARN>'
SELECT zip_with(
       cell_to_boundary_sys(h3_cell, 'lat'),
       cell_to_boundary_sys(h3_cell, 'lng'),
       (x, y)-> ST_Point(x,y)) as coord 
FROM(
USING EXTERNAL FUNCTION lat_lng_to_cell(lat DOUBLE, lng DOUBLE, res INTEGER)

RETURNS BIGINT
LAMBDA '<ARN>'
  SELECT lat_lng_to_cell(latitude, longitude, 4) AS h3_cell
  FROM demo.earthquakes
  WHERE latitude BETWEEN 18 AND 70)