# boss-export

Export data from BOSS s3 bucket in bulk direct to another bucket

This repo contains necessary tools to convert a BOSS dataset to Neuroglancer precomputed format without going through the BOSS endpoint, but by accessing the cuboids directly from S3.

It works by publishing messages (cuboid metadata) to an SQS queue and having a lambda function process those messages, converting and compressing them, into Neuroglancer precomputed format.

You must have read access to the S3 bucket in the BOSS.  Additionally, to be able to compute the s3 keys, you need access to the database IDs for collections/experiments/channels in the BOSS that you wish to convert, as those are used in determining they s3 key names.

## Deployment notes

### Lambda

In addition to the basic lambda execution environment permissions, the lambda role also needs

- s3 getobject from BOSS bucket
- s3 putobject and putobject on destination bucket
- ReceiveMessage/DeleteMessage on SQS

### SQS

Queue visibility needs to be [6 times](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html) the lambda timeout
