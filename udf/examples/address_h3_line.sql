USING EXTERNAL FUNCTION h3_line(start VARCHAR, en VARCHAR)
RETURNS ARRAY(VARCHAR)
LAMBDA  '<ARN>'
SELECT h3_line('883969ab23fffff', '8839681887fffff') 