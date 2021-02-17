package offline

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, BinaryClassificationEvaluator}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import br.ufrj.gta.stream.metrics._
import br.ufrj.gta.stream.schema.flow.Flowtbag
import br.ufrj.gta.stream.util.File

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object DecisionTree {
    def main(args: Array[String]) {

        // Sets dataset file separation string ("," for csv) and label column name
        val sep = ","
        val labelCol = "label"

        // Sets names for colums created during the algorithm execution, containing PCA and regular features
        val pcaFeaturesCol = "pcaFeatures"
        var featuresCol = "features"

        // Defines dataset schema, dataset csv generated by flowtbag https://github.com/DanielArndt/flowtbag
        val schema = Flowtbag.getSchema

        // Checks arguments
        if (args.length < 2) {
            println("Missing parameters")
            sys.exit(1)
        }

        // Path for dataset used from training and testing
        val inputFile = args(0)

        // String cointaining all slave nodes; defaults to localhost if empty
        val slaveNodes = if (args(1) != "local") args(1) else "localhost"

        // Dataset used for model creation and test
        val dataset = args(2)

        // Creates spark session
        val spark = SparkSession
            .builder
            .config(ConfigurationOptions.ES_NODES, slaveNodes)
            .config(ConfigurationOptions.ES_RESOURCE, "spark-offline/classification")
            .appName("Stream")
            .getOrCreate()

        // Reads csv dataset file, fitting it to the schema
        val inputData = spark.read
            .option("sep", sep)
            .option("header", false)
            .schema(schema)
            .csv(inputFile)

        // Creates a single vector column containing all features
        val featurizedData = Flowtbag.featurize(inputData, featuresCol)

        // Splits the dataset on training (70%) and test (30%), with the seed '534661'
        val splitData = featurizedData.randomSplit(Array(0.7, 0.3), 534661)
        val trainingData = splitData(0)
        val testData = splitData(1)

        var startTime = System.currentTimeMillis()

        // Creates a Decision Tree classifier, using the hyperparameters defined previously
        val dt = new DecisionTreeClassifier()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)

        val paramGrid = new ParamGridBuilder()
            //.addGrid(dt.impurity, Array("gini","entropy"))            // Criterion used for information gain calculation
            //.addGrid(dt.maxDepth, Array(3,6,9,12,15,18,21,24,27,30))  // Maximum depth of the tree
            //.addGrid(dt.maxBins, Array(2,4,8,16,32))                  // Maximum number of bins used for discretizing continuous features and for choosing how to split on features at each node
            //.addGrid(dt.minInfoGain, Array(0.0,0.1,0.2,0.3,0.4,0.5))  // Minimum information gain for a split to be considered at a tree node
            //.addGrid(dt.minInstancesPerNode, Array(1,2,5,10,20,40))   // Minimum number of instances each child must have after split
            .build()

        val evaluator = new MulticlassClassificationEvaluator
        //evaluator.setMetricName("weightedPrecision")                  // Uncomment this line to make the evaluator prioritize another metric

        val classifier = new CrossValidator()
            .setEstimator(dt)
            .setEstimatorParamMaps(paramGrid)
            .setEvaluator(evaluator)
            .setNumFolds(10)
            .setSeed(534661)

        // Fits the training data to the classifier, creating the classification model
        val model = classifier.fit(trainingData)

        val hyperparameters = model.bestModel.extractParamMap()

        val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0

        startTime = System.currentTimeMillis()

        // Tests model on the test data
        val prediction = model.transform(testData)

        // Cache model to improve performance
        prediction.cache()

        // Performs an action to accurately measure the test time
        prediction.count()

        val testTime = (System.currentTimeMillis() - startTime) / 1000.0

        // Removes model from cache
        prediction.unpersist()

        // Compute evaluation metrics
        import spark.implicits._
        val predictionAndLabel = prediction.select("prediction","label").as[(Double, Double)].rdd
        val metrics = new MulticlassMetrics(predictionAndLabel)
        val accuracy = metrics.accuracy
        val precision = metrics.precision(1)
        val recall = metrics.recall(1)
        val f1 = metrics.fMeasure(1)

        val aucEvaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
        val auc = aucEvaluator.evaluate(prediction)

        // Creates a DataFrame with the resulting metrics, and send them to ElasticSearch
        val elasticDF = Seq(Row("Decision Tree", accuracy, precision, recall, f1, auc, trainingTime, dataset, hyperparameters.toString()))
        val elasticSchema = List(
          StructField("algorithm", StringType, true),
          StructField("accuracy", DoubleType, true),
          StructField("precision", DoubleType, true),
          StructField("recall", DoubleType, true),
          StructField("f1-score", DoubleType, true),
          StructField("auc", DoubleType, true),
          StructField("training time", DoubleType, true),
          StructField("dataset", StringType, true),
          StructField("hyperparameters", StringType, true))
        val someDF = spark
            .createDataFrame(spark.sparkContext.parallelize(elasticDF),StructType(elasticSchema))
            .saveToEs("spark-offline/classification")

        spark.stop()
    }
}