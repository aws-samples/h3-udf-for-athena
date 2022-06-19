USING EXTERNAL FUNCTION h3_to_center_child(h3 VARCHAR, childres INT) 
RETURNS VARCHAR
LAMBDA 'arn:aws:lambda:eu-west-1:705240738422:function:tlambda'
SELECT h3_to_center_child('843969bffffffff', 5)