#Latest Code

import json
import boto3


client = boto3.client('emr')


def lambda_handler(event, context):
    
    response = client.run_job_flow(
        Name= 'spark_job_cluster',
        LogUri="s3://emrsparklog1/emr/logs",
        ReleaseLabel= 'emr-6.0.0',
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.xlarge',
            'InstanceCount': 1,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
                    },
        Applications = [ {'Name': 'Spark'} ],
        Configurations = [ 
            { 'Classification': 'spark-hive-site',
            'Properties': { 
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole = 'EMR_EC2_DefaultRole',
        ServiceRole = 'EMR_DefaultRole',
        Steps=[
            {
                'Name': 'flow-log-analysis',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            's3://bdmtestcode/app1.py'
                        ]
                }
            }
        ]
    )