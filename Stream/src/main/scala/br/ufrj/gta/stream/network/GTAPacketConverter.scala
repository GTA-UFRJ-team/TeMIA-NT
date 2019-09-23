package br.ufrj.gta.stream.network

import org.apache.spark.ml.param._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import br.ufrj.gta.stream.schema.flow.GTA

class GTAPacketConverter(override val uid: String) extends PacketConverter {
    def convertTCP(df: DataFrame): DataFrame = {
        df.where(df("ip_proto") === "6")
            .groupBy(df("ip_srcaddr").as("srcaddr"), df("ip_dstaddr").as("dstaddr"))
            .agg(
                count("*").as("qtd_pkt_tcp"), // Amount of TCP packets
                countDistinct("tcp_srcport").as("qtd_src_port"), // Amount of source ports
                countDistinct("tcp_dstport").as("qtd_dst_port"), // Amount of destination ports
                count(df("tcp_flagfin")).as("qtd_fin_flag"), // Amount of FIN flags
                count(df("tcp_flagsyn")).as("qtd_syn_flag"), // Amount of SYN flags
                count(df("tcp_flagpsh")).as("qtd_psh_flag"), // Amount of PSH flags
                count(df("tcp_flagack")).as("qtd_ack_flag"), // Amount of ACK flags
                count(df("tcp_flagurg")).as("qtd_urg_flag"), // Amount of URG flags
                lit(0).as("qtd_pkt_udp"), // Amount of UDP packets
                lit(0).as("qtd_pkt_icmp"), // Amount of ICMP packets
                count("*").as("qtd_pkt_ip"), // Amount of IP packets
                lit(0).as("qtd_tos"), // Amount of IP service type // ------------------------------
                avg(df("ip_ttl")).as("ttl_m"), // Average TTL
                avg(df("tcp_hdrlen")).as("header_len_m"), // Average header size
                avg(df("tcp_pktlen")).as("packet_len_m"), // Average packet size
                count(df("ip_flagdf")).as("qtd_do_not_frag"), // Amount of "do not frag" flags
                count(df("ip_flagmf")).as("qtd_more_frag"), // Amount of "more frag" flags
                avg(df("ip_fragoffset")).as("fragment_offset_m"), // Average fragment offset
                count(df("tcp_flagrst")).as("qtd_rst_flag"), // Amount of RST flags
                count(df("tcp_flagecn")).as("qtd_ece_flag"), // Amount of ECN flags
                count(df("tcp_flagcwr")).as("qtd_cwr_flag"), // Amount of CWR flags
                lit(0).as("offset_m"), // Average offset // ------------------------------
                lit(0).as("qtd_t_icmp"), // Amount of ICMP types
                lit(0).as("qtd_cdg_icmp") // Amount of ICMP codes
            )
    }

    def convertUDP(df: DataFrame): DataFrame = {
        df.where(df("ip_proto") === "17")
            .groupBy(df("ip_srcaddr").as("srcaddr"), df("ip_dstaddr").as("dstaddr"))
            .agg(
                lit(0).as("qtd_pkt_tcp"), // Amount of TCP packets
                countDistinct("udp_srcport").as("qtd_src_port"), // Amount of source ports
                countDistinct("udp_dstport").as("qtd_dst_port"), // Amount of destination ports
                lit(0).as("qtd_fin_flag"), // Amount of FIN flags
                lit(0).as("qtd_syn_flag"), // Amount of SYN flags
                lit(0).as("qtd_psh_flag"), // Amount of PSH flags
                lit(0).as("qtd_ack_flag"), // Amount of ACK flags
                lit(0).as("qtd_urg_flag"), // Amount of URG flags
                count("*").as("qtd_pkt_udp"), // Amount of UDP packets
                lit(0).as("qtd_pkt_icmp"), // Amount of ICMP packets
                count("*").as("qtd_pkt_ip"), // Amount of IP packets
                lit(0).as("qtd_tos"), // Amount of IP service type // ------------------------------
                lit(0).as("ttl_m"), // Average TTL
                lit(8).as("header_len_m"), // Average header size
                avg(df("udp_len")).as("packet_len_m"), // Average packet size
                count(df("ip_flagdf")).as("qtd_do_not_frag"), // Amount of "do not frag" flags
                count(df("ip_flagmf")).as("qtd_more_frag"), // Amount of "more frag" flags
                avg(df("ip_fragoffset")).as("fragment_offset_m"), // Average fragment offset
                lit(0).as("qtd_rst_flag"), // Amount of RST flags
                lit(0).as("qtd_ece_flag"), // Amount of ECN flags
                lit(0).as("qtd_cwr_flag"), // Amount of CWR flags
                lit(0).as("offset_m"), // Average offset // ------------------------------
                lit(0).as("qtd_t_icmp"), // Amount of ICMP types
                lit(0).as("qtd_cdg_icmp") // Amount of ICMP codes
            )
    }

    def convertICMP(df: DataFrame): DataFrame = {
        df.where(df("ip_proto") === "1")
            .groupBy(df("ip_srcaddr").as("srcaddr"), df("ip_dstaddr").as("dstaddr"))
            .agg(
                lit(0).as("qtd_pkt_tcp"), // Amount of TCP packets
                lit(0).as("qtd_src_port"), // Amount of source ports
                lit(0).as("qtd_dst_port"), // Amount of destination ports
                lit(0).as("qtd_fin_flag"), // Amount of FIN flags
                lit(0).as("qtd_syn_flag"), // Amount of SYN flags
                lit(0).as("qtd_psh_flag"), // Amount of PSH flags
                lit(0).as("qtd_ack_flag"), // Amount of ACK flags
                lit(0).as("qtd_urg_flag"), // Amount of URG flags
                lit(0).as("qtd_pkt_udp"), // Amount of UDP packets
                count("*").as("qtd_pkt_icmp"), // Amount of ICMP packets
                count("*").as("qtd_pkt_ip"), // Amount of IP packets
                lit(0).as("qtd_tos"), // Amount of IP service type // ------------------------------
                avg(df("ip_ttl")).as("ttl_m"), // Average TTL
                avg(df("ip_hdrlen")).as("header_len_m"), // Average header size
                avg(df("ip_pktlen")).as("packet_len_m"), // Average packet size
                count(df("ip_flagdf")).as("qtd_do_not_frag"), // Amount of "do not frag" flags
                count(df("ip_flagmf")).as("qtd_more_frag"), // Amount of "more frag" flags
                avg(df("ip_fragoffset")).as("fragment_offset_m"), // Average fragment offset
                lit(0).as("qtd_rst_flag"), // Amount of RST flags
                lit(0).as("qtd_ece_flag"), // Amount of ECN flags
                lit(0).as("qtd_cwr_flag"), // Amount of CWR flags
                lit(0).as("offset_m"), // Average offset // ------------------------------
                count(df("icmp_type")).as("qtd_t_icmp"), // Amount of ICMP types
                count(df("icmp_code")).as("qtd_cdg_icmp") // Amount of ICMP codes
            )
    }

    def convert(df: DataFrame): DataFrame = {
        this.convertTCP(df)
            .union(this.convertUDP(df))
            .union(this.convertICMP(df))
    }

    def setLabelValue(df: DataFrame, value: Any): DataFrame = {
        df.withColumn(GTA.getLabelCol, lit(value))
    }
}
