package br.ufrj.gta.stream.classification.anomaly

import scala.collection.mutable.Queue

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.{Predictor, PredictionModel}
//import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._

private[classification] trait EntropyClassifierParams extends Params {
    var numRanges = new IntParam(this, "numRanges", "number of ranges in the histogram parameter (>= 1)")
    var windowSize = new IntParam(this, "windowSize", "window size parameter (>= 1)")
    var entropyCol = new Param[String](this, "entropyCol", "column parameter storing the entropy of the features for a sliding window")

    def getNumRanges: IntParam = this.numRanges

    def getWindowSize: IntParam = this.windowSize

    def getEntropyCol: Param[String] = this.entropyCol

    def setNumRanges(value: Int): this.type = {
        require(value >= 1, s"Number of ranges must be greater or equal to one")
        set(this.numRanges, value)
    }

    def setWindowSize(value: Int): this.type = {
        require(value >= 1, s"Window Size must be greater or equal to one")
        set(this.windowSize, value)
    }

    def setEntropyCol(value: String): this.type = {
        require(!value.trim.isEmpty, s"Entropy column must not be empty")
        set(this.entropyCol, value)
    }
}

class EntropyClassifier(override val uid: String)
    extends Predictor[Vector, EntropyClassifier, EntropyModel]
    with EntropyClassifierParams {

    def this() = this(Identifiable.randomUID("ec"))

    private def log2(value: Double): Double = scala.math.log(value) / scala.math.log(2)

    private def histogram(data: Array[Double], numRanges: Int): Map[Int, Int] = {
        val max = data.max
        val min = data.min

        data.map(e => ((e - min) / (max - min) * numRanges).floor.toInt)
            .groupBy(b => b)
            .map(b => b._1 -> b._2.size)
    }

    private def getWindowEntropy(window: Array[Array[Double]]): Array[Double] = {
        val numRanges = $(this.numRanges)
        val windowSize = $(this.windowSize)
        val numFeatures = window(0).size

        val entropy = Array.ofDim[Double](numFeatures)

        // Transposes the window so that each line corresponds to an Array with the values of a feature
        val transposed = window.transpose[Double]

        for (i <- 0 until transposed.size) {
            for ((k, v) <- this.histogram(transposed(i), numRanges)) {
                val freq = 1.0 * v / windowSize
                entropy(i) -= freq * this.log2(freq)
            }
        }

        entropy
    }

    // TODO: optimize
    private def getSlidingWindowsEntropy(dataset: Dataset[_]): Array[Array[Double]] = {
        val datasetSize = dataset.count
        val windowSize = $(this.windowSize)
        val featuresCol = $(this.featuresCol)

        val featuresDataset = dataset.select(dataset(featuresCol)).cache()
        val numFeatures = featuresDataset.first().getAs[Vector](0).size

        val slidingWindows = Array.ofDim[Array[Double]]((datasetSize - windowSize + 1).toInt)

        val iterator = featuresDataset.toLocalIterator()

        // Using a queue (FIFO) to store the features Vector of a window
        var windowFeatures = Queue[Array[Double]]()
        var i = 0
        var w = 0
        while (iterator.hasNext) {
            windowFeatures.enqueue(iterator.next.getAs[Vector](featuresCol).toArray)
            i += 1

            // Enqueues the elements until an entire window (windowSize features) has been collected
            if (i - windowSize == w) {
                // Before the calculation of the entropy, pops out an element to keep it with exactly windowSize elements
                if (w != 0) {
                    windowFeatures.dequeue
                }

                slidingWindows(w) = this.getWindowEntropy(windowFeatures.toArray)
                w += 1
            }
        }

        featuresDataset.unpersist()

        slidingWindows
    }

    override def train(dataset: Dataset[_]): EntropyModel = {
        new EntropyModel(this.uid, this.getSlidingWindowsEntropy(dataset))
    }

    override def copy(extra: ParamMap): EntropyClassifier = defaultCopy(extra)
}

private[stream] class EntropyModel(
        override val uid: String,
        val entropy: Array[Array[Double]])
    extends PredictionModel[Vector, EntropyModel] {

    override val numFeatures = this.entropy.size

    override def copy(extra: ParamMap): EntropyModel = {
        copyValues(new EntropyModel(this.uid, this.entropy)).setParent(this.parent)
    }

    override def predict(features: Vector): Double = {
        0.0
    }
}
