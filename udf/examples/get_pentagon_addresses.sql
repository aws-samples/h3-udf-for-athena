USING EXTERNAL FUNCTION get_pentagon_adresses(res INT) 
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
SELECT get_pentagon_adresses(10);
