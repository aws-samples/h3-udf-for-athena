WITH dataset AS(
    USING EXTERNAL FUNCTION get_h3_unidirectional_edges_from_hexagon(edge BIGINT)
    RETURNS ARRAY(BIGINT)
    LAMBDA '<ARN'
    SELECT get_h3_unidirectional_edges_from_hexagon(599988766760763391) as edges)
select edge from dataset cross join unnest(edges) as t(edge)