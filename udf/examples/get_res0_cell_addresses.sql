WITH dataset AS(
    USING EXTERNAL FUNCTION get_res0_cells(dummy VARCHAR) 
    RETURNS ARRAY(VARCHAR)
    LAMBDA '<ARN>'
    SELECT get_res0_cells('') as cells)
SELECT cell FROM dataset CROSS JOIN unnest(cells) AS t(cell)