package br.ufrj.gta.stream.network.fields

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object UDPHeaderFields {
    def getSchema: StructType = {
        new StructType()
            .add("udp_srcport", "int") // UDP source port number
            .add("udp_dstport", "int") // UDP destination port number
            .add("udp_len", "int") // UDP length
    }
}
