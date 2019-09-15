package flow

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import br.ufrj.gta.stream.network.GTAPacketConverter
import br.ufrj.gta.stream.schema.packet.GTA

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
            .toDF(GTA.getColNames: _*)

        val gtaConverter = new GTAPacketConverter("gtapc")

        val outputDataStream = packetsDataStream.writeStream
            .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
                gtaConverter.convert(packetsDataStream).write
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
