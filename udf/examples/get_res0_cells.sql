USING EXTERNAL FUNCTION get_res0_cells(dummy INT) 
RETURNS ARRAY(BIGINT)
LAMBDA '<ARN>'
SELECT get_res0_cells(5)