package br.ufrj.gta.stream.network.fields

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object TcpHeaderFields {
    def getSchema: StructType = {
        new StructType()
            .add("srcport", "int") // The source port number
            .add("dstport", "int") // The destination port number
            .add("ackflag", "int") // Acknowledgement flag
            .add("cwrflag", "int") // Congestion window reduced flag
            .add("ecnflag", "int") // Explicit congestion notification flag
            .add("finflag", "int") // Last packet flag
            .add("pushflag", "int") // Push flag
            .add("resetflag", "int") // Reset flag
            .add("synflag", "int") // Synchronize flag
            .add("urgflag", "int") // Urgent flag
            .add("hdrlen", "int") // TCP header length
            .add("pktlen", "int") // TCP packet length
    }
}
