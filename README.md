## H3 UDF for Athena

This repository contains example code to support the blog post [Add a H3 UDF to Athena](). The UDF makes it straightforward for [Amazon Athena](https://aws.amazon.com/athena/) to find out which [Uber H3](https://eng.uber.com/h3/) hexagon a pair of (lat, long) coordinates are in. This can be used for subsequent analysis and visualisation. 

We include code for the [AWS Lambda](https://aws.amazon.com/lambda/) function that powers the new Athena UDF. Also included is an example Jupyter Notebook which may be used in an [Amazon SageMaker Notbook Instance](https://docs.aws.amazon.com/sagemaker/latest/dg/nbi.html) to render a choropleth map.

![Map](https://github.com/aws-samples/h3-udf-for-athena/blob/master/media/earthquake_map.png)


### Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

### License

This library is licensed under the MIT-0 License. See the LICENSE file.