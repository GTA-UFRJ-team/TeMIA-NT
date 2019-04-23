package br.ufrj.gta.stream

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import br.ufrj.gta.stream.schema.GTA
import br.ufrj.gta.stream.anomaly.{MeanVarianceClassifier, MeanVarianceModel}

object Stream {
    def main(args: Array[String]) {
        val sep = ","
        val maxFilesPerTrigger = 1
        val threshold = 1.0
        val featuresCol = "features"
        val labelCol = "label"

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

        val Array(trainingData, testData) = GTA.featurize(inputDataStatic, featuresCol).randomSplit(Array(0.7, 0.3))

        val mv = new MeanVarianceClassifier()

        mv.setFeaturesCol(featuresCol)
        mv.setLabelCol(labelCol)

        val model = mv.fit(trainingData)
        val result = model.transform(testData)

        result.cache()

        println("# of test cases")
        println(result.count())

        println("# of legitimates")
        println(result.where(result(mv.getPredictionCol) === 0.0).count())

        println("# of anomalies")
        println(result.where(result(mv.getPredictionCol) === 1.0).count())*/

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

        val trainingData = GTA.featurize(inputDataStatic, featuresCol)

        val mv = new MeanVarianceClassifier()

        mv.setFeaturesCol(featuresCol)
        mv.setLabelCol(labelCol)

        val model = mv.fit(trainingData)

        val result = model.transform(GTA.featurize(inputDataStream))

        val outputDataStream = result.drop(result(featuresCol)).writeStream
            //.trigger(Trigger.Once())
            .outputMode("append")
            .option("checkpointLocation", outputPath + "checkpoints/")
            .format("csv")
            .option("path", outputPath)
            .start()

        outputDataStream.awaitTermination()
    }
}
