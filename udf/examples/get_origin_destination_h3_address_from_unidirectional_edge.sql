WITH dataset AS(
    USING EXTERNAL FUNCTION get_h3_unidirectional_edges_from_hexagon(edge VARCHAR)
    RETURNS ARRAY(VARCHAR)
    LAMBDA '<ARN>'
    SELECT get_h3_unidirectional_edges_from_hexagon('853969abfffffff') as edges)
SELECT edge FROM dataset CROSS JOIN unnest(edges) as t(edge)