package com.yztsoft.sosu.esclient;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @classname: EsClient
 * @description:
 * @author: Shi Shijie
 * @create: 2018/11/20
 **/
public class EsClient {

    private static Logger logger = LogManager.getLogger(EsClient.class);

    public final static Client CLIENT = initClient();

    private static Client initClient() {
        if (null != CLIENT) {
            return CLIENT;
        }
        Client client = null;
        try {
            logger.info("创建Elasticsearch Client 开始");
            Settings settings = Settings.settingsBuilder().put("cluster.name","yzt_index")
                    .put("client.transport.sniff", true).build();
            TransportClient tranClien = TransportClient.builder().settings(settings).addPlugin(DeleteByQueryPlugin.class).build();
            String[] ips = "127.0.0.1:9300".split(",");
            if(ips==null || ips.length<1){
                throw new NullPointerException("elasticsearch server ip list is null");
            }
            for (String ip : ips) {
                if(StringUtils.isNotBlank(ip)){
                    String[] address=ip.split(":");
                    tranClien.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(address[0]),Integer.valueOf(address[1])));
                }
            }
            client=tranClien;
            logger.info("创建Elasticsearch Client 结束");
        } catch (Exception e) {
            logger.error("创建Client异常", e);
        }

        return client;
    }

    /**
     * 关闭
     */
    public static void close(){
        if(null != CLIENT){
            try {
                CLIENT.close();
            } catch (Exception e) {
                logger.error("关闭Client异常", e);
            }
        }
    }
}
