SELECT cell, face FROM  (
    USING EXTERNAL FUNCTION get_icosahedron_faces(h3 BIGINT) 
    RETURNS ARRAY(INT) 
    LAMBDA '<ARN>'
    SELECT 581975902628347903 as cell, get_icosahedron_faces(581975902628347903) AS faces
) CROSS JOIN unnest(faces) AS t(face)