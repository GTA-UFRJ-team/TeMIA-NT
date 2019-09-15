package br.ufrj.gta.stream.schema.flow

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.feature.VectorAssembler

import br.ufrj.gta.stream.schema.FlowSchema

object GTA extends FlowSchema {
    def getSchema: StructType = {
        new StructType()
            .add("srcaddr", "string") // (string) The source IP address
            .add("dstaddr", "string") // (string) The destination IP address
            .add("qtd_pkt_tcp", "int") // Amount of TCP packets
            .add("qtd_src_port", "int") // Amount of source ports
            .add("qtd_dst_port", "int") // Amount of destination ports
            .add("qtd_fin_flag", "int") // Amount of FIN flags
            .add("qtd_syn_flag", "int") // Amount of SYN flags
            .add("qtd_psh_flag", "int") // Amount of PSH flags
            .add("qtd_ack_flag", "int") // Amount of ACK flags
            .add("qtd_urg_flag", "int") // Amount of URG flags
            .add("qtd_pkt_udp", "int") // Amount of UDP packets
            .add("qtd_pkt_icmp", "int") // Amount of ICMP packets
            .add("qtd_pkt_ip", "int") // Amount of IP packets
            .add("qtd_tos", "int") // Amount IP service type
            .add("ttl_m", "int") // Average TTL
            .add("header_len_m", "int") // Average header size
            .add("packet_len_m", "int") // Average packet size
            .add("qtd_do_not_frag", "int") // Amount of "do not frag" flags
            .add("qtd_more_frag", "int") // Amount of "more frag" flags
            .add("fragment_offset_m", "int") // Average fragment offset
            .add("qtd_rst_flag", "int") // Amount of RST flags
            .add("qtd_ece_flag", "int") // Amount of ECE flags
            .add("qtd_cwr_flag", "int") // Amount of CWR flags
            .add("offset_m", "int") // Average offset
            .add("qtd_t_icmp", "int") // Amount of ICMP types
            .add("qtd_cdg_icmp", "int") // Amount of ICMP codes
            .add("label", "int") // class label
    }

    override def getNumFeatures: Int = {
        24
    }

    private def getFeaturesVector(featuresCol: String): VectorAssembler = {
        new VectorAssembler()
            .setInputCols(
                getSchema
                    .fieldNames
                    .toList
                    .takeRight(25)
                    .dropRight(1)
                    .toArray)
            .setOutputCol(featuresCol)
    }

    def featurize(df: DataFrame, featuresCol: String = "features"): DataFrame = {
        getFeaturesVector(featuresCol).transform(df)
    }
}
