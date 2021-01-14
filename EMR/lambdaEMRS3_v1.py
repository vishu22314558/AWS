import json
import boto3
import datetime


def lambda_handler(event, context):
    print("Creating EMR")
    connection = boto3.client("emr", region_name="us-east-1")
    print(event)
    cluster_id = connection.run_job_flow(
        Name="Emr_Spark",
        ServiceRole="EMR_DefaultRole",
        JobFlowRole="EMR_EC2_DefaultRole",
        VisibleToAllUsers=True,
        LogUri="s3://emrsparklog1/emr/logs",
        ReleaseLabel="emr-6.2.0",
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Slave nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                }
            ],
            "Ec2KeyName": "EMR_test",
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False
        },
        Applications=[{
            "Name": "Spark"

        }],
        Configurations=[
                {
                    'Classification': 'spark-hive-site',
                    'Properties': {
                        'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                    }
                }
            ],
        Steps=[{
            "Name": "StepNameEMR",
            "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "–-deploy-mode",
                    "cluster",
                    "–-master",
                    "yarn",
                    "–-conf",
                    "spark.yarn.submit.waitAppCompletion=true",
                    "s3://bdmtestcode/app1.py"
                    
                ]
            }
        }],
    )
    print(cluster_id["JobFlowId"])
    return "Started cluster {}".format(cluster_id)


