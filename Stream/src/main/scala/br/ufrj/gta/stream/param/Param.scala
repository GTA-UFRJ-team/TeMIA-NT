package br.ufrj.gta.stream.param

import org.apache.spark.ml.param._

trait HasThresholdParams extends Params {
    var threshold = new DoubleParam(this, "threshold", "threshold parameter (>= 0)")

    def getThreshold: DoubleParam = this.threshold

    def setThreshold(value: Double): this.type = {
        require(value >= 0.0, s"Threshold value must not be negative")
        set(this.threshold, value)
    }
}

trait HasNumFeaturesParams extends Params {
    var numFeatures = new IntParam(this, "numFeatures", "number of features (>= 1)")

    def getNumFeatures: IntParam = this.numFeatures

    def setNumFeatures(value: Int): this.type = {
        require(value >= 1, s"Number of features must be greater than or equal to one")
        set(this.numFeatures, value)
    }
}
