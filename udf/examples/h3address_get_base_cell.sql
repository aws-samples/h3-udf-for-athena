USING EXTERNAL FUNCTION h3_get_base_cell(h3Address VARCHAR) 
RETURNS INT 
LAMBDA '<arn>'
SELECT h3_get_base_cell(h3) AS basecell FROM(
USING EXTERNAL FUNCTION geo_to_h3_address(lat DOUBLE, lng DOUBLE, res INT) 
RETURNS VARCHAR 
LAMBDA '<arn>'
SELECT geo_to_h3_address(43.552847, 7.017369, 10) AS h3)
-- 28