package br.ufrj.gta.stream.classification.anomaly

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.{Predictor, PredictionModel}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.rdd.RDDFunctions
import org.apache.spark.sql.{Row, Dataset, DataFrame}
import org.apache.spark.sql.functions._

import br.ufrj.gta.stream.param._
import br.ufrj.gta.stream.util.StreamUtils

case class EntropyLimits(lower: Array[Double], upper: Array[Double])

private[classification] trait EntropyClassifierParams extends Params {
    var numRanges = new IntParam(this, "numRanges", "number of ranges in the histogram parameter (>= 1)")
    var windowSize = new IntParam(this, "windowSize", "window size parameter (>= 1)")

    def getNumRanges: IntParam = this.numRanges

    def getWindowSize: IntParam = this.windowSize

    def setNumRanges(value: Int): this.type = {
        require(value >= 1, s"Number of ranges must be greater than or equal to one")
        set(this.numRanges, value)
    }

    def setWindowSize(value: Int): this.type = {
        require(value >= 1, s"Window Size must be greater than or equal to one")
        set(this.windowSize, value)
    }
}

class EntropyClassifier(override val uid: String)
    extends Predictor[Vector, EntropyClassifier, EntropyModel]
    with EntropyClassifierParams with HasThresholdParams with HasNumFeaturesParams {

    def this() = this(Identifiable.randomUID("ec"))

    private def getFeaturesEntropy(dataset: Dataset[_]): EntropyLimits = {
        val numRanges = $(this.numRanges)
        val windowSize = $(this.windowSize)
        val threshold = $(this.threshold)
        val numFeatures = $(this.numFeatures)
        val featuresCol = $(this.featuresCol)

        val slidingRDD = new RDDFunctions(dataset.select(dataset(featuresCol)).rdd).sliding(windowSize)

        val slidingWindows = slidingRDD.map { window =>
            val features = window.toArray.map(r => r.getAs[Vector](featuresCol).toArray)

            val entropy = Array.ofDim[Double](numFeatures)

            // Transposes the window so that each line corresponds to an Array with the values of a feature
            val transposed = features.transpose[Double]

            var i = 0

            for (i <- 0 until transposed.size) {
                for ((k, v) <- StreamUtils.histogram(transposed(i), numRanges)) {
                    val freq = 1.0 * v / windowSize
                    entropy(i) -= freq * StreamUtils.log2(freq)
                }
            }

            entropy
        }.cache()

        var lowerLimit = Array.ofDim[Double](numFeatures)
        var upperLimit = Array.ofDim[Double](numFeatures)

        // TODO: optimize
        var i = 0

        for (i <- 0 until numFeatures) {
            val (buckets, counts) = slidingWindows.map(
                f => f(i)
            ).histogram(numRanges)

	        val index = counts.indexOf(counts.max)

	        if (index == buckets.size - 1) {
    	        lowerLimit(i) = buckets(index) - threshold
    	        upperLimit(i) = lowerLimit(i) + (lowerLimit(i) - buckets(index - 1)) + threshold
	        } else {
	            lowerLimit(i) = buckets(index) - threshold
	            upperLimit(i) = buckets(index + 1) + threshold
	        }
        }

        slidingWindows.unpersist()

        EntropyLimits(lowerLimit, upperLimit)
    }

    override def train(dataset: Dataset[_]): EntropyModel = {
        new EntropyModel(this.uid, this.getFeaturesEntropy(dataset))
            .setFeaturesCol($(this.featuresCol))
            .setWindowSize($(this.windowSize))
            .setNumRanges($(this.numRanges))
    }

    override def copy(extra: ParamMap): EntropyClassifier = defaultCopy(extra)
}

private[stream] class EntropyModel(
        override val uid: String,
        val limits: EntropyLimits)
    extends PredictionModel[Vector, EntropyModel]
    with EntropyClassifierParams {

    require(this.limits.lower.size == this.limits.upper.size, s"The sizes of lower and upper limits arrays must be equal")

    override val numFeatures = this.limits.lower.size

    override def copy(extra: ParamMap): EntropyModel = {
        copyValues(new EntropyModel(this.uid, this.limits)).setParent(this.parent)
    }

    override def transformImpl(dataset: Dataset[_]): DataFrame = {
        val numRanges = $(this.numRanges)
        val windowSize = $(this.windowSize)
        val featuresCol = $(this.featuresCol)

        val numFeatures = this.numFeatures

        val featuresDataset = dataset.select(dataset(featuresCol))
        val schema = featuresDataset.schema
        val sparkSession = featuresDataset.sparkSession

        val windowsEntropy = featuresDataset.rdd
            .zipWithIndex()
            .map(t => ((t._2 * 1.0 / windowSize).toInt, t._1))
            .groupByKey()
            .map { t =>
                val features = t._2.toArray.map(r => r.getAs[Vector](0).toArray)

                val entropy = Array.ofDim[Double](numFeatures)

                // Transposes the window so that each line corresponds to an Array with the values of a feature
                val transposed = features.transpose[Double]

                var i = 0

                for (i <- 0 until transposed.size) {
                    for ((k, v) <- StreamUtils.histogram(transposed(i), numRanges)) {
                        val freq = 1.0 * v / windowSize
                        entropy(i) -= freq * StreamUtils.log2(freq)
                    }
                }

                Row(Vectors.dense(entropy))
            }

        super.transformImpl(sparkSession.createDataFrame(windowsEntropy, schema))
    }

    override def predict(features: Vector): Double = {
        var i = 0

	    for (i <- 0 until this.numFeatures) {
	        if (features(i) < this.limits.lower(i) || features(i) > this.limits.lower(i)) {
    		    return 1.0
	        }
	    }

	    0.0
    }
}
