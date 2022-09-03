USING EXTERNAL FUNCTION grid_ring_unsafe(origin VARCHAR, k INT) 
RETURNS ARRAY(VARCHAR) 
LAMBDA '<ARN>'
SELECT grid_ring_unsafe('8a3969ab2037fff', 1) AS hexring
