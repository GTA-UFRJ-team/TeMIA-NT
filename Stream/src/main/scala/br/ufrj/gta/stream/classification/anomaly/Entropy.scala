package br.ufrj.gta.stream.classification.anomaly

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.{Predictor, PredictionModel}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._

private[classification] trait EntropyClassifierParams extends Params {
    var windowSize = new IntParam(this, "windowSize", "window size parameter (>= 1)")
    var entropyCol = new Param[String](this, "entropyCol", "column parameter storing the entropy of the features for a sliding window")

    def getWindowSize: IntParam = this.windowSize

    def getEntropyCol: Param[String] = this.entropyCol

    def setWindowSize(value: Int): this.type = {
        require(value >= 1, s"Window Size value must be greater or equal to one")
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

    private def getFeaturesEntropy(dataset: Dataset[_]): Vector = {
        // TODO: calculate the entropy of each windows
        val addEntropy: (Vector) => Vector = (features: Vector) => {
            val numFeatures = features.size

            val entropy = features.toDense.toArray.map { f =>
                val freq = f / numFeatures
                - 1.0 * freq * scala.math.log(freq) / scala.math.log(2)
            }

            Vectors.dense(entropy).toSparse
        }

        val addEntropyUDF = udf(addEntropy)

        val entropyDataset = dataset.withColumn($(this.entropyCol), addEntropyUDF(dataset($(this.featuresCol))))

        // TODO: sum the entropy of all features
        val row = dataset/*.select(
            Summarizer.metrics("sum")
                .summary(entropyDataset($(this.entropyCol)))
                .as("summary"))
            .select("summary.sum")*/
            .first()

        row.getAs[Vector](0)
    }

    override def train(dataset: Dataset[_]): EntropyModel = {
        val entropy = this.getFeaturesEntropy(dataset)

        new EntropyModel(this.uid, entropy)
    }

    override def copy(extra: ParamMap): EntropyClassifier = defaultCopy(extra)
}

private[stream] class EntropyModel(
        override val uid: String,
        val entropy: Vector)
    extends PredictionModel[Vector, EntropyModel] {

    override val numFeatures = this.entropy.size

    override def copy(extra: ParamMap): EntropyModel = {
        copyValues(new EntropyModel(this.uid, this.entropy)).setParent(this.parent)
    }

    override def predict(features: Vector): Double = {
        0.0
    }
}
