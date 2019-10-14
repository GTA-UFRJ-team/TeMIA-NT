package flow

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import br.ufrj.gta.stream.metrics._
import br.ufrj.gta.stream.network.GTAPacketConverter
import br.ufrj.gta.stream.schema.flow.{GTA => FlowGTA}
import br.ufrj.gta.stream.schema.packet.{GTA => PacketGTA}

object Flow {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        if (args.length < 6) {
            println("Missing parameters")
            sys.exit(1)
        }

        val defaultTriggerProcessingTime = "5 seconds"

        val terminationTime = args(0).toLong
        val progressFilename = args(1)
        val kafkaServer = args(2)
        val packetsTopic = args(3)
        val flowsTopic = args(4)
        val labelValue = args(5).toInt
        val triggerProcessingTime: String = try {
            args(6)
        } catch {
            case e: Exception => defaultTriggerProcessingTime
        }

        val inputDataStream = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaServer)
            .option("subscribe", packetsTopic)
            .load()

        val valueDataStream = inputDataStream
            .select(inputDataStream("value").cast("string"))

        val packetsDataStream = valueDataStream
            .withColumn("fields", split(valueDataStream("value"), ","))
            .select(PacketGTA.getFieldsRange.map(c => col("fields").getItem(c).as(s"col$c")): _*)
            .toDF(PacketGTA.getColNames: _*)

        val streamingMetrics = new StreamingMetrics(StreamingMetrics.names)

        spark.streams.addListener(streamingMetrics.getListener)

        val gtaConverter = new GTAPacketConverter("gtapc")

        val outputDataStream = packetsDataStream.writeStream
            .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
                val flowDF = gtaConverter.setLabelValue(gtaConverter.convert(batchDF), labelValue)

                val kakfaFlowDF = flowDF
                    .select(array_join(array(FlowGTA.getColNames.map(c => flowDF(c).cast("string")): _*), ",").as("value"))

                kakfaFlowDF.write
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaServer)
                    .option("topic", flowsTopic)
                    .save()
            }
            .trigger(Trigger.ProcessingTime(triggerProcessingTime))
            .start()

        outputDataStream.awaitTermination(terminationTime)

        streamingMetrics.export(progressFilename, Metrics.FormatCsv)

        spark.stop()
    }
}
