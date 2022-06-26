USING EXTERNAL FUNCTION exact_edge_length(edge VARCHAR, unit VARCHAR) 
RETURNS DOUBLE
LAMBDA '<ARN>'
SELECT exact_edge_length('16a3969ab218ffff', 'rads')