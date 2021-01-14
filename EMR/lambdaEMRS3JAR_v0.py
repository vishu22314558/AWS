import json
import boto3
import datetime
def lambda_handler(event, context):
   print("Creating EMR")
   connection = boto3.client('emr', region_name='us-east-1')
   print(event)
    cluster_id = connection.run_job_flow(
       Name='Emr_Spark', 
       LogUri='s3://sparkuser/emr/logs', 
       ReleaseLabel='emr-5.21.0', 
       Applications=[
   	    {'Name': 'Hadoop'}, 
   	    {'Name': 'Hive'}, 
   	    {'Name': 'Spark'}
   	],
    	Instances={
       	'InstanceGroups': [
   	    	{
   		    	'Name': 'Master nodes', 
   			    'Market': 'ON_DEMAND', 
   				'InstanceRole': 'MASTER', 
       			'InstanceType': 'r4.8xlarge', 
   	    		'InstanceCount': 1, 
   		    	'EbsConfiguration': {
   			    	'EbsBlockDeviceConfigs': [
   				    	{
   					        	'VolumeSpecification':{
   						         'VolumeType': 'gp2', 
   							     'SizeInGB': 400
       						},
   	    					'VolumesPerInstance': 2
   		    			},
   			    	],
   
       				'EbsOptimized': True,
   	    		}
   		    },
   			{
       			'Name': 'Slave nodes', 
   	    		'Market': 'SPOT', 
   		    	'InstanceRole': 'CORE', 
   			    'InstanceType': 'r4.8xlarge', 
   				'InstanceCount': 1, 
       			'EbsConfiguration': {
   	    			'EbsBlockDeviceConfigs': [
   		    			{
   			    			'VolumeSpecification': {
   				    			'VolumeType': 'gp2',
   					    		'SizeInGB': 500
   						    },
   							'VolumesPerInstance': 2
       					},
   	    			],
   		    		'EbsOptimized': True,
   			    },
   				'AutoScalingPolicy': { 
       				'Constraints': {
   	    				'MinCapacity': 2, 
   		    			'MaxCapacity': 20
   			    	},
   				    'Rules': [
   						{
       						'Name': 'Compute-scale-up', 
   	    					'Description': 'scale up on YARNMemory',
   		    				'Action': {
   			    				 'SimpleScalingPolicyConfiguration': {
   				    				  'AdjustmentType': 'CHANGE_IN_CAPACITY', 
   					    			  'ScalingAdjustment': 1, 
   						    		  'CoolDown': 300
   							    }
   
       						},
   	    					'Trigger': {
   		    					'CloudWatchAlarmDefinition' :{
   			    					'ComparisonOperator': 'LESS_THAN', 
   				    				'EvaluationPeriods': 120, 
   					    			'MetricName': 'YARNMemoryAvailablePercentage', 
   						    		'Namespace': 'AWS/ElasticMapReduce', 
   							    	'Period': 300, 
   								    'Statistic': 'AVERAGE', 
   									'Threshold': 20, 
       								'Unit': 'PERCENT'
   	    						}
   		    				}
   			    		},
   				    	{
   					    	'Name': 'Compute-scale-down',
   						    'Description':'scale down on YARNMemory', 
   							'Action': {
       							'SimpleScalingPolicyConfiguration' : {
   	    						'AdjustmentType': 'CHANGE_IN_CAPACITY', 
   		    					'ScalingAdjustment': -1, 
   			    				'CoolDown': 300
   				    	}
   					    },
   						'Trigger':{
       						'CloudWatchAlarmDefinition': {
   	    					'ComparisonOperator': 'LESS_THAN', 
   		    				'EvaluationPeriods': 125, 
   			    			'MetricName': 'YARNMemoryAvailablePercentage',
   				    		'Namespace': 'AWS/ElasticMapReduce', 
   					    	'Period': 250,
   						    'Statistic': 'AVERAGE', 
   							'Threshold': 85, 
       						'Unit': 'PERCENT'
   	    				}
   		    		}
   			    }
   			]
       	}
   	}
       ],
   
   	'KeepJobFlowAliveWhenNoSteps': True, 
   	'TerminationProtected': True,
   	'AutoTerminate': True, 
   	'Ec2KeyName': 'EMR_test.pem',
   	'Ec2SubnetId': 'subnet-0198a20fbd7c5f549', 
   	'EmrManagedMasterSecurityGroup': 'sg-0141dee2ca266f83b',
   	'EmrManagedSlaveSecurityGroup':  'sg-072d020138130e430'
   	},
        Steps=[
       {
           'Name': 'spark-submit',
           'ActionOnFailure': 'TERMINATE_CLUSTER',
           'HadoopJarStep': {
               'Jar': 's3://sparkuser/jar/example.jar',
               'Args': [
               	'spark-submit','--executor-memory', '1g', '--driver-memory', '500mb',
               	'--class', 'spark.exercise_1'
                   
               ]
           }
       }
   ],
   	AutoScalingRole='EMR_AutoScaling_DefaultRole', 
   	VisibleToAllUsers=True, 
   	JobFlowRole='EMR_EC2_DefaultRole', 
   	ServiceRole= 'EMR_DefaultRole', 
   	EbsRootVolumeSize=100, 
   	Tags=[
   		{
   			'Key': 'NAME', 
   			'Value': 'Emr_Spark',
   		},
   	],
   )   
	print (cluster_id['JobFlowId']) 