-- get all h3 that covers Toulouse, nantes, Lille, Paris, Nice , and then compact them.

USING EXTERNAL FUNCTION compact_address(h3array ARRAY(VARCHAR))
RETURNS ARRAY(VARCHAR)
LAMBDA 'arn:aws:lambda:<region>:<account>:function:<functionname>' 
SELECT compact_address(polyfillh3s) FROM (
USING EXTERNAL FUNCTION polyfill_address(polygonWKT VARCHAR, res INT)
RETURNS ARRAY(VARCHAR)
LAMBDA 'arn:aws:lambda:<region>:<account>:function:<functionname>' 
SELECT polyfill_address('POLYGON ((43.604652 1.444209, 47.218371 -1.553621, 50.62925 3.05726, 48.864716 2.349014, 43.6961 7.27178, 3.604652 1.444209))', 4) AS polyfillh3s)
