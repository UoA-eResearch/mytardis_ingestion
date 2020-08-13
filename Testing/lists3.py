import boto3

endpoint_url = 'https://store.nectar.auckland.ac.nz:443'
bucket = 'mech_tests'
s3_client = boto3.client('s3', endpoint_url=endpoint_url)
print(s3_client)
Files = s3_client.list_objects_v2(Bucket=bucket)
print(Files)
for filename in Files['Contents']:
    print(filename)
