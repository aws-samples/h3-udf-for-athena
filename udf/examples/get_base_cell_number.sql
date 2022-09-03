USING EXTERNAL FUNCTION get_base_cell_number(h3 BIGINT) 
RETURNS INT 
LAMBDA '<ARN>'
SELECT get_base_cell_number(h3) AS basecell FROM(
USING EXTERNAL FUNCTION lat_lng_to_cell(lat DOUBLE, lng DOUBLE, res INT) 
RETURNS BIGINT 
LAMBDA '<ARN>'
SELECT lat_lng_to_cell(43.552847, 7.017369, 10) AS h3)
-- 28