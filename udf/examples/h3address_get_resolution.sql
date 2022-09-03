
USING EXTERNAL FUNCTION get_resolution(h3Address VARCHAR) 
RETURNS INT 
LAMBDA '<ARN>'

SELECT get_resolution(h3) AS res FROM(
USING EXTERNAL FUNCTION lat_lng_to_cell_address(lat DOUBLE, lng DOUBLE, res INT) 
RETURNS VARCHAR 
LAMBDA '<ARN>'
SELECT lat_lng_to_cell_address(43.552847, 7.017369, 10) AS h3)
-- 10