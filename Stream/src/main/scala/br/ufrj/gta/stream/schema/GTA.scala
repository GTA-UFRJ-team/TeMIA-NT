package br.ufrj.gta.stream.schema

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector

object GTA {
    def getSchema(): StructType = {
        new StructType()
            .add("srcip", "string") // (string) The source ip address
            .add("srcport", "int") // The source port number
            .add("dstip", "string") // (string) The destination ip address
            .add("dstport", "int") // The destination port number
            .add("proto", "int") // The protocol (ie. TCP = 6, UDP = 17)
            .add("total_fpackets", "int") // Total packets in the forward direction
            .add("total_fvolume", "int") // Total bytes in the forward direction
            .add("total_bpackets", "int") // Total packets in the backward direction
            .add("total_bvolume", "int") // Total bytes in the backward direction
            .add("min_fpktl", "int") // The size of the smallest packet sent in the forward direction (in bytes)
            .add("mean_fpktl", "int") // The mean size of packets sent in the forward direction (in bytes)
            .add("max_fpktl", "int") // The size of the largest packet sent in the forward direction (in bytes)
            .add("std_fpktl", "int") // The standard deviation from the mean of the packets sent in the forward direction (in bytes)
            .add("min_bpktl", "int") // The size of the smallest packet sent in the backward direction (in bytes)
            .add("mean_bpktl", "int") // The mean size of packets sent in the backward direction (in bytes)
            .add("max_bpktl", "int") // The size of the largest packet sent in the backward direction (in bytes)
            .add("std_bpktl", "int") // The standard deviation from the mean of the packets sent in the backward direction (in bytes)
            .add("min_fiat", "int") // The minimum amount of time between two packets sent in the forward direction (in microseconds)
            .add("mean_fiat", "int") // The mean amount of time between two packets sent in the forward direction (in microseconds)
            .add("max_fiat", "int") // The maximum amount of time between two packets sent in the forward direction (in microseconds)
            .add("std_fiat", "int") // The standard deviation from the mean amount of time between two packets sent in the forward direction (in microseconds)
            .add("min_biat", "int") // The minimum amount of time between two packets sent in the backward direction (in microseconds)
            .add("mean_biat", "int") // The mean amount of time between two packets sent in the backward direction (in microseconds)
            .add("max_biat", "int") // The maximum amount of time between two packets sent in the backward direction (in microseconds)
            .add("std_biat", "int") // The standard deviation from the mean amount of time between two packets sent in the backward direction (in microseconds)
            .add("duration", "int") // The duration of the flow (in microseconds)
            .add("min_active", "int") // The minimum amount of time that the flow was active before going idle (in microseconds)
            .add("mean_active", "int") // The mean amount of time that the flow was active before going idle (in microseconds)
            .add("max_active", "int") // The maximum amount of time that the flow was active before going idle (in microseconds)
            .add("std_active", "int") // The standard deviation from the mean amount of time that the flow was active before going idle (in microseconds)
            .add("min_idle", "int") // The minimum time a flow was idle before becoming active (in microseconds)
            .add("mean_idle", "int") // The mean time a flow was idle before becoming active (in microseconds)
            .add("max_idle", "int") // The maximum time a flow was idle before becoming active (in microseconds)
            .add("std_idle", "int") // The standard devation from the mean time a flow was idle before becoming active (in microseconds)
            .add("sflow_fpackets", "int") // The average number of packets in a sub flow in the forward direction
            .add("sflow_fbytes", "int") // The average number of bytes in a sub flow in the forward direction
            .add("sflow_bpackets", "int") // The average number of packets in a sub flow in the backward direction
            .add("sflow_bbytes", "int") // The average number of packets in a sub flow in the backward direction
            .add("fpsh_cnt", "int") // The number of times the PSH flag was set in packets travelling in the forward direction (0 for UDP)
            .add("bpsh_cnt", "int") // The number of times the PSH flag was set in packets travelling in the backward direction (0 for UDP)
            .add("furg_cnt", "int") // The number of times the URG flag was set in packets travelling in the forward direction (0 for UDP)
            .add("burg_cnt", "int") // The number of times the URG flag was set in packets travelling in the backward direction (0 for UDP)
            .add("total_fhlen", "int") // The total bytes used for headers in the forward direction.
            .add("total_bhlen", "int") // The total bytes used for headers in the backward direction.
            .add("dscp", "int") // The first set DSCP field for the flow.
            .add("label", "int") // class label
    }

    def getNumFeatures(): Int = {
        40
    }

    def getFeaturesRange(): Range = {
        0 until 40
    }

    private def getFeaturesVector(outputCol: String): VectorAssembler = {
        new VectorAssembler()
            .setInputCols(
                GTA.getSchema
                    .fieldNames
                    .toList
                    .takeRight(41)
                    .dropRight(1)
                    .toArray)
            .setOutputCol(outputCol)
    }

    def featurize(df: DataFrame, outputCol: String = "features"): DataFrame = {
        GTA.getFeaturesVector(outputCol).transform(df)
    }
}
