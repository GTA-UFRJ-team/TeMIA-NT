package flow

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Flow {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        val kafkaServer = "localhost:9092"
        val packetsTopic = "packets"
        val flowsTopic = "flows"
        val triggerProcessingTime = "5 seconds"

        val inputDataStream = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaServer)
            .option("subscribe", packetsTopic)
            .load()

        val packetsDataStream = inputDataStream
            .select(inputDataStream("value").cast("string"))
            .withColumn("value", split(inputDataStream("value"), ","))

        val outputDataStream = packetsDataStream.writeStream
            .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
                // TODO
                batchDF.write
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaServer)
                    .option("topic", flowsTopic)
                    .save()
            }
            .trigger(Trigger.ProcessingTime(triggerProcessingTime))
            .start()

        outputDataStream.awaitTermination()

        spark.stop()
    }
}
