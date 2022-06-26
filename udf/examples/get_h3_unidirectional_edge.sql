USING EXTERNAL FUNCTION get_h3_unidirectional_edge(origin VARCHAR, destination VARCHAR)
RETURNS VARCHAR
LAMBDA '<ARN>'
SELECT get_h3_unidirectional_edge('8a3969ab218ffff', '8a3969ab2187fff') 