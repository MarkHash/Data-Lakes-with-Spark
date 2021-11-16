## Terminal command to launch EMR cluster

```
aws emr create-cluster \
--name spark-cluster \
--use-default-roles \
--release-label emr-5.28.0 \
--instance-count 3 \
--applications Name=Spark Name=Zeppelin Name=JupyterEnterpriseGateway Name=Hive \
--bootstrap-actions Path=s3://udacity-nanodegree-dataengineer-markhash/bootstrap_emr.sh \
--ec2-attributes KeyName=spark-cluster,SubnetId=subnet-05edda13a138b9bd5 \
--instance-type m5.xlarge \
--profile spark-cluster
```

## SSH command to access EMR master
ssh -i "xxx.pem" hadoop@"Public IPv4 DNS for EMR master"

