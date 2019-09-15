package br.ufrj.gta.stream.network

import org.apache.spark.ml.param._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class GTAPacketConverter(override val uid: String) extends PacketConverter {
    def convertIP(df: DataFrame): DataFrame = {
        df.groupBy(df("ip_srcaddr"), df("ip_dstaddr"))
            .agg(
                count("*").as("qtd_pkt_ip"), // Number of IP packets
                avg(df("ip_ttl")).as("ttl_m"), // Average TTL
                count(df("ip_flagdf").as("qtd_do_not_frag")), // Number of "do not frag" flags
                count(df("ip_flagmf").as("qtd_more_frag")), // Number of "more frag" flags
                avg(df("ip_fragoffset").as("fragment_offset_m")), // Average fragment offset
                avg(df("ip_hdrlen").as("header_len_m")), // Average header size
                avg(df("ip_pktlen").as("packet_len_m")) // Average packet size
            )
    }

    def convertTCP(df: DataFrame): DataFrame = {
        df.where(df("ip_proto") === "6")
            .groupBy(df("ip_srcaddr"), df("ip_dstaddr"))
            .agg(
                count("*").as("qtd_pkt_tcp"), // Number of TCP packets
                count(df("tcp_flagfin").as("qtd_fin_flag")), // Number of FIN flags
                count(df("tcp_flagsyn").as("qtd_syn_flag")), // Number of SYN flags
                count(df("tcp_flagpsh").as("qtd_psh_flag")), // Number of PSH flags
                count(df("tcp_flagack").as("qtd_ack_flag")), // Number of ACK flags
                count(df("tcp_flagurg").as("qtd_urg_flag")), // Number of URG flags
                count(df("tcp_flagrst").as("qtd_rst_flag")), // Number of RST flags
                count(df("tcp_flagecn").as("qtd_ece_flag")), // Number of ECN flags
                count(df("tcp_flagcwr").as("qtd_cwr_flag")), // Number of CWR flags
                avg(df("tcp_hdrlen").as("header_len_m")), // Average header size
                avg(df("tcp_pktlen").as("packet_len_m")) // Average packet size
            )
    }

    def convertUDP(df: DataFrame): DataFrame = {
        df.where(df("ip_proto") === "17")
            .groupBy(df("ip_scraddr"), df("ip_dstaddr"))
            .agg(
                count("*").as("qtd_pkt_udp"), // Number of UDP packets
                avg(df("udp_hdrlen").as("header_len_m")), // Average header size
                avg(df("udp_pktlen").as("packet_len_m")) // Average packet size
            )
    }

    def convertICMP(df: DataFrame): DataFrame = {
        df.where(df("ip_proto") === "1")
            .groupBy(df("ip_scraddr"), df("ip_dstaddr"))
            .agg(
                count("*").as("qtd_pkt_udp"), // Number of ICMP packets
                count(df("icmp_type").as("qtd_t_icmp")), // Number of ICMP types
                count(df("icmp_code").as("qtd_cdg_icmp")) // Number of ICMP codes
            )
    }

    def convert(df: DataFrame): DataFrame = {
        val ipNumCols = 8
        val tcpNumCols = 10
        val udpNumCols = 2
        val icmpNumCols = 2

        var lowerCols = 1
        var upperCols = ipNumCols

        val ipRange = lowerCols to upperCols
        lowerCols = upperCols

        upperCols += tcpNumCols
        val tcpRange = lowerCols to upperCols
        lowerCols = upperCols

        upperCols += udpNumCols
        val udpRange = lowerCols to upperCols
        lowerCols = upperCols

        upperCols += icmpNumCols
        val icmpRange = lowerCols to upperCols

        var flowDF = this.convertIP(df)

        if (get(this.getConvertTCP).isEmpty) {
            flowDF = flowDF.union(this.convertTCP(df))
        }

        if (get(this.getConvertUDP).isEmpty) {
            flowDF = flowDF.union(this.convertUDP(df))
        }

        if (get(this.getConvertICMP).isEmpty) {
            flowDF = flowDF.union(this.convertICMP(df))
        }

        flowDF
    }

    override def copy(extra: ParamMap): GTAPacketConverter = defaultCopy(extra)
}
