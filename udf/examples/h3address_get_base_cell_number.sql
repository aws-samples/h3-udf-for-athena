USING EXTERNAL FUNCTION get_base_cell_number(h3Address VARCHAR) 
RETURNS INT 
LAMBDA '<ARN>'
SELECT get_base_cell_number(h3) AS basecell FROM(
USING EXTERNAL FUNCTION lat_lng_to_cell_address(lat DOUBLE, lng DOUBLE, res INT) 
RETURNS VARCHAR 
LAMBDA '<ARN>'
SELECT lat_lng_to_cell_address(43.552847, 7.017369, 10) AS h3)
-- 28