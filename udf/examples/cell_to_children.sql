SELECT cell, child FROM (
    USING EXTERNAL FUNCTION cell_to_children(h3 BIGINT, childres INT) 
    RETURNS ARRAY(BIGINT) 
    LAMBDA '<ARN>'
    SELECT 595485172502102015 as cell, cell_to_children(595485172502102015, 5) as children
) CROSS JOIN unnest(children) AS t(child)