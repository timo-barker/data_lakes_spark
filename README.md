# data_lakes_with_spark

We'll be creating an EMR cluster for the exercise.

### Prerequesites

1. Make sure you first install `awscli` and provision it with command `aws configure`. This will store your Access Key ID, Secret Access Key, and some basic configuration settings in unencypted, extensionless, plain text files at ~/.aws/credentials and ~/.aws/config. View your current settings with `aws configure list` and `aws aim list-users`. Repeat this process if your config or access credentials change.
2. Create an AWS EC2 key pair. Key pairs ensure that you alone have access to the instances that you launch. Go to the AWS EC2 console, click Key Pairs. On the Key Pairs page, click Create Key Pair. In the Create Key Pair dialog box, enter a name for your key pair, such as, *MyKeyPair*. Click Create. Save the resulting PEM file in a safe location.
3. Add a new rule to allow ingress to Port 22 for SSH connections from your IP address. Go to the AWS EMR console, select the name of your cluster. In the Summary tab, go to the Security and access section and click the link for `Security groups for Master`. When the screen refreshes, select the Security group ID that corresponds to the Security group name for `ElasticMapReduce-master`. If there are no inbound rules for SSH, click the Edit inbound rules button. Scroll down and click the Add rule button. Select `SSH` from the first drop down menu. Select `My IP` from the second drop down manu. Click Save rules at the bottom of the screen. Repeat this process if your IP address changes.

### Initiate a Cluster

Once it's installed and configured, run the script below to launch a cluster. Revise anything in the `<>` to match your filename.

YOUR_CLUSTER_NAME: required, anything you'd like!  
YOUR_KEY_NAME: required, your IAM key name that is saved under .ssh/ directory.  
YOUR_BOOTSTRAP_FILENAME: optional, should be your bootstrap file, executable (.sh file) in an accessible S3 location. If you aren't going to use the bootstrap file, you can removed `--bootstrap-actions` tag.
 
```
aws emr create-cluster --name <YOUR_CLUSTER_NAME> --use-default-roles  --release-label emr-5.28.0 --instance-count 2 --applications Name=Spark Name=Hadoop Name=Livy Name=Zeppelin  --bootstrap-actions Path=<YOUR_BOOTSTRAP_FILENAME> --ec2-attributes KeyName=<YOUR_KEY_NAME> --instance-type m5.xlarge --instance-count 3 --auto-terminate
```

This will give you something like this..

```
{
    "ClusterId": "j-2PZ79NHXO7YYX",
    "ClusterArn": "arn:aws:elasticmapreduce:us-east-2:027631528606:cluster/j-2PZ79NHXO7YYX"
}
```

Go to AWS EMR console from your web browser, then check if the cluster is showing up. Or you can type;

`aws emr describe-cluter --cluster-id <CLUSTER_ID FROM ABOVE>`

For example, I would do `aws emr describe-cluster --cluster-id j-2PZ79NHXO7YYX` to see if this cluster is ready to go.

### SSH py files to Master
