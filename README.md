Develop Realtime Streaming Ingestion from MongoDB Atlas into Hudi Datalake with Glue, kinesis and eventbridge

![image](https://user-images.githubusercontent.com/39345855/219885203-953ace3a-4285-4023-82eb-6c21a68ebfc6.png)

## Macro Steps 
* Create MongoDB Atlas Clusters 
* create Trigger to fwd events to event bridge 
* create event bridge rules to foreward events into kinesis 
* processing streaming data using glue 3.0 and insert into hudi data lake 


#### Settings on Glue 4.0 on Job parameters 

```
--conf   spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false
--datalake-formats  hudi
```
