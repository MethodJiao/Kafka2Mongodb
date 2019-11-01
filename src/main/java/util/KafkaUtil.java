package util;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * @Author Method.Jiao
 * @Date 2019/10/19 13:36
 */
public class KafkaUtil {
    public static KafkaConsumer<String, String> getConnect() {
        ConnectConfig connectConfig = new ConnectConfig();
        //连接到 kafka 服务
        Properties props = new Properties();
        props.put("bootstrap.servers", connectConfig.getKafkaIp() + ":" + connectConfig.getKafkaPort());
        props.put("group.id", "groupA");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList("flume1"));

        return kafkaConsumer;
    }
}
