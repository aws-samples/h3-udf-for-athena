SELECT cell, face FROM  (
    USING EXTERNAL FUNCTION get_icosahedron_faces(h3 VARCHAR) 
    RETURNS ARRAY(INT) 
    LAMBDA '<ARN>'
    SELECT 581975902628347903 as cell, get_icosahedron_faces('81397ffffffffff') AS faces
) CROSS JOIN unnest(faces) AS t(face)