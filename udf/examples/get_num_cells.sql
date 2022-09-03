
USING EXTERNAL FUNCTION get_num_cells(res INT) 
RETURNS BIGINT
LAMBDA '<ARN>'
SELECT get_num_cells(1), get_num_cells(10)
