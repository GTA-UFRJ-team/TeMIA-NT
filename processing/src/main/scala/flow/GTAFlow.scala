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

object GTAFlow {
    def main(args: Array[String]) {

        // Creates spark session
        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        // Checks arguments
        if (args.length < 6) {
            println("Missing parameters")
            sys.exit(1)
        }

        // Sets flow duration
        val defaultTriggerProcessingTime = "5 seconds"

        // Time the program will remain actively monitoring Kafka
        val terminationTime = args(0).toLong

        // Filename for saving results obtained during the abstraction progress
        val progressFilename = args(1)

        // Kafka server address
        val kafkaServer = args(2)

        // Kafka topic for incoming network packets
        val packetsTopic = args(3)

        // Kafka topic name for resulting flows, after the abstraction process
        val flowsTopic = args(4)

        // Set label on abstracted flows; can assume them to be either legitimate (0) or malicious (1)
        val labelValue = args(5).toInt

        // Sets total number of PCA features; optional
        val triggerProcessingTime: String = try {
            args(6)
        } catch {
            case e: Exception => defaultTriggerProcessingTime
        }

        // Reads new data arriving at the Kafka packets topic
        val inputDataStream = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaServer)
            .option("subscribe", packetsTopic)
            .load()

        // Receives flow value data from the Kafka packets topic, and converts it to String
        val valueDataStream = inputDataStream
            .select(inputDataStream("value").cast("string"))

        // Creates a DataFrame containing the network packets, adapting the data to fit the Spark abstraction format
        val packetsDataStream = valueDataStream
            .withColumn("fields", split(valueDataStream("value"), ","))
            .select(PacketGTA.getFieldsRange.map(c => col("fields").getItem(c).as(s"col$c")): _*)
            .toDF(PacketGTA.getColNames: _*)

        // Starts collecting streaming metrics (including "Input rows per second" and "Processed rows per second")
        val streamingMetrics = new StreamingMetrics(StreamingMetrics.names)
        spark.streams.addListener(streamingMetrics.getListener)

        // Converts the network packets into flows, and sends them to the Kafka flows topic
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

        // Wait until timeout to stop the online abstraction tool
        outputDataStream.awaitTermination(terminationTime)

        // Saves streaming metrics on a csv file
        streamingMetrics.export(progressFilename, Metrics.FormatCsv)

        spark.stop()
    }
}
