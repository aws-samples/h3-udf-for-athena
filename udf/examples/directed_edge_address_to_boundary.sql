WITH points AS(
	USING EXTERNAL FUNCTION directed_edge_to_boundary(edge VARCHAR) 
	RETURNS ARRAY(VARCHAR) 
	LAMBDA '<ARN>'
SELECT directed_edge_to_boundary('16a3969ab218ffff') ps)
SELECT ST_geometry_from_text(point) FROM points CROSS JOIN unnest(ps) AS t(point)