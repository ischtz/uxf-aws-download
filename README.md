# uxf-aws-download

A command line utility to download experiment results stored by the Unity Experiment Framework (>= 2.0) from AWS DynamoDB.

*Disclaimer: While I try to ensure that this is free of bugs, I assume no responsibility whatsoever if this tool doesn't work as described, racks up your AWS bill, or ends up filling your harddrive with garbage.*


# Usage

The tool is called from the command line 

```
python uxf_aws_download.py <arguments>
```

with command line arguments specified below. Note that you must specify an AWS region (and your AWS credentials must allow access to DynamoDB in this region). Credentials are taken from the default AWS config if no other arguments are specified. Alternatively, you can select a profile using --profile or directly pass access and secret keys from the command line (not recommended in scripts).

```
usage: uxf_aws_download.py [-h] -r AWS_REGION [-p] [-t] [-f]
                           [--profile PROFILE] [--access ACCESS]
                           [--secret SECRET]
                           experiment

A tool to download Unity Experiment Framework data from AWS DynamoDB

positional arguments:
  experiment         Experiment name to download

optional arguments:
  -h, --help         show this help message and exit
  -r AWS_REGION      AWS region to use for DynamoDB (required)
  -p                 Only retrieve and list participant details
  -t                 Also download Tracker table (caution: this might use a
                     lot of data!)
  -f                 Create a subfolder using experiment name
  --profile PROFILE  Use specific profile from AWS credentials file
  --access ACCESS    AWS access key to use
  --secret SECRET    AWS secret key to use
  ```