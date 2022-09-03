WITH dataset AS( 
    USING EXTERNAL FUNCTION get_directed_edge_origin_destination(edge VARCHAR)
    RETURNS ARRAY(VARCHAR)
    LAMBDA '<ARN>'
    SELECT get_directed_edge_origin_destination('16a3969ab218ffff') edges)
SELECT edge FROM dataset CROSS JOIN unnest(edges) as t(edge)