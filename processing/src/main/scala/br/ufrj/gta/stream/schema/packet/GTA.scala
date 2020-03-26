package br.ufrj.gta.stream.schema.packet

import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.feature.VectorAssembler

import br.ufrj.gta.stream.schema.PacketSchema

object GTA extends PacketSchema {

    // Spark abstraction features
    def getSchema: StructType = {
        new StructType()
            .add("ip_srcaddr", "string") // (string) The source IP address
            .add("ip_dstaddr", "string") // (string) The destination IP address
            .add("ip_proto", "int") // The IP protocol (ie. TCP = 6, UDP = 17)
            .add("ip_ttl", "int") // IP time to travel
            .add("ip_flagdf", "int") // IP "do not frag" flag
            .add("ip_flagmf", "int") // IP "more frag" flag
            .add("ip_fragoffset", "int") // IP fragment offset
            .add("ip_hdrlen", "int") // IP header length
            .add("ip_pktlen", "int") // IP packet length
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
            .add("udp_srcport", "int") // UDP source port number
            .add("udp_dstport", "int") // UDP destination port number
            .add("udp_len", "int") // UDP length
            .add("icmp_type", "int") // ICMP type
            .add("icmp_code", "int") // ICMP code
    }
}
