USING EXTERNAL FUNCTION get_hexagon_edge_length_avg(h3 INT, unit VARCHAR) 
RETURNS DOUBLE
LAMBDA '<ARN>'
SELECT get_hexagon_edge_length_avg(1, 'km')