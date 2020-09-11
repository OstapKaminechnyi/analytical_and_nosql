// Databricks notebook source
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val ehConnectionString = "Endpoint=sb://iot-vm.servicebus.windows.net/;SharedAccessKeyName=policy-eventhub;SharedAccessKey=Un8xkgtXTmElrKhlPq4sKz+Wcy9Z1H09chDF/xywDfE=;EntityPath=databricks-event-hub"

val eventHubsConf = EventHubsConf(ehConnectionString)
  .setStartingPosition(EventPosition.fromStartOfStream)
  .setConsumerGroup("$Default")

val eventhubStream = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()


// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

var streamingSelectDF = 
  eventhubStream
   .select(get_json_object(($"body").cast("string"), "$.body.vendorid").alias("vendorid"),
           get_json_object(($"body").cast("string"), "$.body.dropoff_latitude").alias("dropoff_latitude"),
           get_json_object(($"body").cast("string"), "$.body.dropoff_longitude").alias("dropoff_longitude"),
           get_json_object(($"body").cast("string"), "$.body.total_amount").alias("total_amount")
          )
  
streamingSelectDF.createOrReplaceTempView("dataTable")
display(streamingSelectDF)

// COMMAND ----------

val dataTable = spark.sql("select * from dataTable")
display(dataTable.select("*"))

// COMMAND ----------

import org.apache.spark.sql

val fileSystemName = "lab8";
var storageAccountName = "labiot9";
val accountKey = "EyR33atfS02mXHU5tvQiimVPexo1MR+SfChatHWLeRJppkWFghstk/1JE6IjKpBGYQc5CAOAKvydFIyVikXThw==";

spark.conf.set(s"fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net", accountKey)
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true");

val vendor_count = streamingSelectDF.groupBy("vendorid").count()
display(vendor_count)
vendor_count.writeStream
    .queryName("vendor_count")
    .outputMode("complete")
    .format("memory")
    .start()

// COMMAND ----------

val selected = spark.sql("select * from vendor_count")
display(selected)

// COMMAND ----------

selected
   .coalesce(1).write.csv("abfss://" + fileSystemName + "@" + storageAccountName + ".dfs.core.windows.net/mydatacsv3");
