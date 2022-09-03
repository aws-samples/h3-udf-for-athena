WITH dataset AS(
    USING EXTERNAL FUNCTION origin_to_directed_edges(edge VARCHAR)
    RETURNS ARRAY(VARCHAR)
    LAMBDA '<ARN>'
    SELECT origin_to_directed_edges('853969abfffffff') as edges)
SELECT edge FROM dataset CROSS JOIN unnest(edges) as t(edge)