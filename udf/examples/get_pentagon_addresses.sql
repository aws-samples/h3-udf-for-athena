
WITH dataset AS(
USING EXTERNAL FUNCTION get_pentagon_addresses(res INT) 
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
SELECT get_pentagon_addresses(10) AS pentagon_addresses)
SELECT pentagon_address FROM dataset CROSS JOIN unnest(pentagon_addresses) AS t(pentagon_address) 
