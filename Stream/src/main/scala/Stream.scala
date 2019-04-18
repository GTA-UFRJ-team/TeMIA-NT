package br.ufrj.gta.stream

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import br.ufrj.gta.stream.schema._
import br.ufrj.gta.stream.anomaly._

object Stream {
    def main(args: Array[String]) {
        val sep: String = ","
        val maxFilesPerTrigger: Int = 1
        val threshold: Double = 1.0

        val schema = GTA.getSchema

        val spark = SparkSession.builder.appName("Stream").getOrCreate()

        // Testing one cross validation round
        /*if (args.length < 1) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputFile: String = args(0)

        val inputDataStatic = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputFile)

        val Array(trainingData, testData) = GTA.featurize(inputDataStatic).randomSplit(Array(0.7, 0.3))

        val model = spark.sqlContext.sparkContext.broadcast(MeanVariance.model(trainingData, threshold))

        println("# of anomalies")
        println(MeanVariance.getAnomalies(model, testData, GTA.getFeaturesRange).collect().length)

        println("# of legitimates")
        println(MeanVariance.getLegitimates(model, testData, GTA.getFeaturesRange).collect().length)

        println("# of tested streams")
        val result = MeanVariance.test(model, testData, GTA.getFeaturesRange)
        println(result.collect()(0))*/

        // Testing using Structured Stream
        if (args.length < 3) {
            println("Missing parameters")
            sys.exit(1)
        }

        val inputTrainingFile: String = args(0)
        val inputPath: String = args(1)
        val outputPath: String = args(2)

        val inputDataStream = spark.readStream
            //.option("maxFilesPerTrigger", maxFilesPerTrigger)
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputPath)

        val inputDataStatic = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputTrainingFile)

        val trainingData = GTA.featurize(inputDataStatic)

        val model = spark.sqlContext.sparkContext.broadcast(MeanVariance.model(trainingData, threshold))

        val testData = GTA.featurize(inputDataStream)

        val classified = MeanVariance.test(model, testData, GTA.getFeaturesRange)

        val outputDataStream = classified.drop(classified("features")).writeStream
            //.trigger(Trigger.Once())
            .outputMode("append")
            .option("checkpointLocation", outputPath + "checkpoints/")
            .format("csv")
            .option("path", outputPath)
            .start()

        outputDataStream.awaitTermination()
    }
}
