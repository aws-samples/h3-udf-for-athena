SELECT cell, descendant FROM (
    USING EXTERNAL FUNCTION cell_to_descendants(h3 BIGINT, childres INT) 
    RETURNS ARRAY(BIGINT) 
    LAMBDA '<ARN>'
    SELECT 595485172502102015 as cell, cell_to_descendants(595485172502102015, 2) as descendants
) CROSS JOIN unnest(descendants) AS t(descendant)