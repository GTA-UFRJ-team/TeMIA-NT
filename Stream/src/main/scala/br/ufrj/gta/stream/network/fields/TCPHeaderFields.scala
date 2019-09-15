package br.ufrj.gta.stream.network.fields

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object TCPHeaderFields {
    def getSchema: StructType = {
        new StructType()
            .add("tcp_srcport", "int") // TCP source port number
            .add("tcp_dstport", "int") // TCP destination port number
            .add("tcp_flagack", "int") // TCP acknowledgement flag
            .add("tcp_flagcwr", "int") // TCP congestion window reduced flag
            .add("tcp_flagecn", "int") // TCP explicit congestion notification flag
            .add("tcp_flagfin", "int") // TCP last packet flag
            .add("tcp_flagpsh", "int") // TCP push flag
            .add("tcp_flagrst", "int") // TCP reset flag
            .add("tcp_flagsyn", "int") // TCP synchronize flag
            .add("tcp_flagurg", "int") // TCP urgent flag
            .add("tcp_hdrlen", "int") // TCP header length
            .add("tcp_pktlen", "int") // TCP packet length
    }
}
