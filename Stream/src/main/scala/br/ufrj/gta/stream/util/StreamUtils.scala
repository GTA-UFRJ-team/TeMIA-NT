package br.ufrj.gta.stream.util

object StreamUtils {
    def log2(value: Double): Double = scala.math.log(value) / scala.math.log(2)

    def histogram(data: Array[Double], numRanges: Int): Map[Int, Int] = {
	    val min = data.min
	    val max = data.max

        data.map(e => ((e - min) / (max - min) * numRanges).floor.toInt)
            .groupBy(b => b)
            .map(b => b._1 -> b._2.size)
    }
}
