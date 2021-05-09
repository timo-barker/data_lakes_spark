# data_lakes_with_spark

A fictitious music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this Udacity Data Engineering project, we will build an ETL pipeline for a data lake hosted on AWS S3. We will load data from S3, process the data into analytics tables using Spark on AWS EMR, and load them back into S3. We will deploy this Spark process on a cluster using AWS.

However, before we can perform the actual ETL pipeline work, we must first create an EMR cluster for the exercise. This requires setup the first time. <!--This learning path for this exercise is not Administrator, Security, or DevOps focused.--> The instructions below are provided <!--as copy/paste directions -->for readying our AWS resources<!-- quickly with little regard to perfection-->.

## Prerequesites

1. Make sure you first install `awscli` and provision it with command `aws configure`. This will store your Access Key ID, Secret Access Key, and some basic configuration settings in unencypted, extensionless, plain text files at ~/.aws/credentials and ~/.aws/config. View your current settings with `aws configure list` and `aws aim list-users`. Repeat this process if your config or access credentials change.
2. Create an AWS EC2 key pair. Key pairs ensure that you alone have access to the instances that you launch. Go to the AWS EC2 console, click Key Pairs. On the Key Pairs page, click Create Key Pair. In the Create Key Pair dialog box, enter a name for your key pair, such as, *MyKeyPair*. Click Create. Save the resulting PEM file in a safe location.
3. Add a new rule to allow ingress to Port 22 for SSH connections from your IP address. Go to the AWS EMR console, select the name of your cluster. In the Summary tab, go to the Security and access section and click the link for `Security groups for Master`. When the screen refreshes, select the Security group ID that corresponds to the Security group name for `ElasticMapReduce-master`. Click the Edit inbound rules button. Scroll down and click the Add rule button. Select `SSH` from the first drop down menu. Select `My IP` from the second drop down manu. Click Save rules at the bottom of the screen. Repeat this process if your IP address changes.
4. Create your own S3 bucket. Go to the AWS S3 console, click the Create bucket button. Choose a publicly visible and unique name for your bucket, for example *MyProcessedFilesProject4*. Select the region closest to your data source. Keep all the default settings and click the Create bucket button. Since the s3://udacity-dend/ data is located in us-west-2, I am also creating my S3 bucket in the same region.
5. Create a VPC if one is not already setup. Go to the AWS VPC console, click the Create VPC button.
6. Open up your S3 bucket for access. We are going to allow access to to it from within the VPC. Go to the AWC VPC console and click on Your VPCs. Copy your VPC ID. Next, go to the AWS S3 console, and click on your S3 bucket name. Click on the Access Points tab. Click the Create access points button. Give your Access point a name and paste your VPC ID in the text box where it asks.
7. Create the EMR default roles in AIM by running command `aws emr create-default-roles`.

## Create an EMR Cluster

Once it's installed and configured, run the script below to launch a cluster. Revise anything in the `<>` to match your filename.

--name 'SparkDemoCluster': required, anything you'd like!  
--ec2-attributes KeyName=AWS_ECS_Demo_2: required, your IAM key name that is saved under .ssh/ directory.  
--bootstrap-actions Path=<YOUR_BOOTSTRAP_FILENAME>: optional, should be your bootstrap file, executable (.sh file) in an accessible S3 location. If you aren't going to use the bootstrap file, you can removed `--bootstrap-actions` tag.
--auto-terminate: : optional.
 
```
aws emr create-cluster --name 'SparkDemoCluster' --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark Name=Hadoop Name=Livy Name=Zeppelin Name=Hive --ec2-attributes KeyName=AWS_ECS_Demo_2 --instance-type m5.xlarge --region us-west-2 --auto-terminate 
```
for cheaper pricing:
```
aws emr create-cluster \
--name 'SparkDemoCluster' \
--applications Name=Hadoop Name=Spark Name=Livy Name=Zeppelin Name=JupyterEnterpriseGateway \
--region us-west-2 \
--ec2-attributes AvailabilityZone=us-west-2b \
--ec2-attributes KeyName=AWS_ECS_Demo_2 \
--release-label emr-5.33.0 \
--service-role EMR_DefaultRole \
--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
--instance-fleets \
InstanceFleetType=MASTER,TargetSpotCapacity=1,\
InstanceTypeConfigs=['{InstanceType=m5.xlarge,BidPrice=0.075}'] \
InstanceFleetType=CORE,TargetSpotCapacity=2,\
InstanceTypeConfigs=['{InstanceType=m5.xlarge,BidPrice=0.075}'] 
```

This will give you something like this..

```
{
    "ClusterId": "j-2PZ79NHXO7YYX",
    "ClusterArn": "arn:aws:elasticmapreduce:us-west-2:027631528606:cluster/j-2PZ79NHXO7YYX"
}
```

Go to AWS EMR console from your web browser, then check if the cluster is showing up. Or you can type;

`aws emr describe-cluster --cluster-id j-2PZ79NHXO7YYX`

Scroll down to `END` and exit by pressing the `Q` key. Keep checking to see if this cluster is ready to go. Eventually, you should see a JSON attribute with the endpoint like such: 

```
"MasterPublicDnsName": "ec2-3-139-93-181.us-west-2.compute.amazonaws.com"
```

With the settings above, we are running 1 master, 2 core, and 0 task for a total of 3 nodes in our cluster. The [Amazon EMR Pricing](https://aws.amazon.com/emr/pricing) for m5.xlarge is USD $0.192/hr for on demand and USD $0.048/hr for spot. So if we run our cluster for 15 minutes, our cost for the assignment should be (USD $0.192 x 3 x 15/60) = USD $0.14.

## SSH py files to Master

1. Connect using the SSH protocol. You can run the commands shown in the figure below in your terminal.
```
ssh -v -i .aws/AWS_ECS_Demo_2.pem hadoop@ec2-3-139-93-181.us-west-2.compute.amazonaws.com
```
When you have verified a successful connection, you will be logged into the EMR shell. 
```
Last login: Sat May  8 21:32:23 2021

       __|  __|_  )
       _|  (     /   Amazon Linux AMI
      ___|\___|___|

https://aws.amazon.com/amazon-linux-ami/2018.03-release-notes/
56 package(s) needed for security, out of 102 available
Run "sudo yum update" to apply all updates.
                                                                    
EEEEEEEEEEEEEEEEEEEE MMMMMMMM           MMMMMMMM RRRRRRRRRRRRRRR    
E::::::::::::::::::E M:::::::M         M:::::::M R::::::::::::::R   
EE:::::EEEEEEEEE:::E M::::::::M       M::::::::M R:::::RRRRRR:::::R 
  E::::E       EEEEE M:::::::::M     M:::::::::M RR::::R      R::::R
  E::::E             M::::::M:::M   M:::M::::::M   R:::R      R::::R
  E:::::EEEEEEEEEE   M:::::M M:::M M:::M M:::::M   R:::RRRRRR:::::R 
  E::::::::::::::E   M:::::M  M:::M:::M  M:::::M   R:::::::::::RR   
  E:::::EEEEEEEEEE   M:::::M   M:::::M   M:::::M   R:::RRRRRR::::R  
  E::::E             M:::::M    M:::M    M:::::M   R:::R      R::::R
  E::::E       EEEEE M:::::M     MMM     M:::::M   R:::R      R::::R
EE:::::EEEEEEEE::::E M:::::M             M:::::M   R:::R      R::::R
E::::::::::::::::::E M:::::M             M:::::M RR::::R      R::::R
EEEEEEEEEEEEEEEEEEEE MMMMMMM             MMMMMMM RRRRRRR      RRRRRR
                                                                    
[hadoop@ip-172-31-49-60 ~]$ 
```
2. 

```
scp -v -i <.pem-file> <Local-Path> hadoop@<EMR-MasterNode-Endpoint>:~<EMR-path>
```

## ETL Pipeline
    
1.  Read data from S3
    
    -   Song data:  `s3://udacity-dend/song_data`
    -   Log data:  `s3://udacity-dend/log_data`
    
    The script reads song_data and load_data from S3.
    
2.  Process data using spark
    
    Transforms them to create five different tables listed below : 
    #### Fact Table
	 **songplays**  - records in log data associated with song plays i.e. records with page  `NextSong`
    -   _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

	#### Dimension Tables
	 **users**  - users in the app
		Fields -   _user_id, first_name, last_name, gender, level_
		
	 **songs**  - songs in music database
    Fields - _song_id, title, artist_id, year, duration_
    
	**artists**  - artists in music database
    Fields -   _artist_id, name, location, lattitude, longitude_
    
	  **time**  - timestamps of records in  **songplays**  broken down into specific units
    Fields -   _start_time, hour, day, week, month, year, weekday_
    
3.  Load it back to S3
    
    Writes them to partitioned parquet files in table directories on S3.
