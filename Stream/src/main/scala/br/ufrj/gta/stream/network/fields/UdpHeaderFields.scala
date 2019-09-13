package br.ufrj.gta.stream.network.fields

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object UdpHeaderFields {
    def getSchema: StructType = {
        new StructType()
            .add("srcport", "int") // The source port number
            .add("dstport", "int") // The destination port number
            .add("udplen", "int") // UDP length
    }
}
