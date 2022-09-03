SELECT cell, child FROM (
    USING EXTERNAL FUNCTION cell_to_children(h3 VARCHAR, childres INT) 
    RETURNS ARRAY(VARCHAR) 
    LAMBDA '<ARN>'
    SELECT '843969bffffffff' as cell, cell_to_children('843969bffffffff', 5) as children
) CROSS JOIN unnest(children) AS t(child)