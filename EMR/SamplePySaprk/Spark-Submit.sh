spark-submit --master yarn --deploy-mode cluster --num-executors 1 --executor-cores 1 --executor-memory 10g  's3://bdmtestcode/app1.py'

spark-submit --deploy-mode cluster --master yarn --num-executors 5 --executor-cores 5 --executor-memory 20g –conf spark.yarn.submit.waitAppCompletion=false wordcount.py s3://inputbucket/input.txt s3://outputbucket/
#Note that I am also setting the property spark.yarn.submit.waitAppCompletion with the step definitions. When this property is set to false, the client submits the application and exits, not waiting for the application to complete. This setting allows you to submit multiple applications to be executed simultaneously by the cluster and is only available in cluster mode.

spark-submit --master yarn --deploy-mode client --num-executors 2 --executor-cores 2 --executor-memory 10g  's3://bdmtestcode/app1.py'
# Client to debug the error 
