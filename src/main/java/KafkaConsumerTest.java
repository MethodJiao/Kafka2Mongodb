import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Title: KafkaConsumerTest
 * Description:
 * kafka消费者 demo
 * Version:1.0.0
 *
 * @author Method.Jiao
 * @date 2019年10月18日
 */
public class KafkaConsumerTest implements Runnable {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final String kafkaServer = "localhost:9092";
    private ConsumerRecords<String, String> consumerRecords;

    //kafa配置信息
    public KafkaConsumerTest(String topicName, String groupID) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer);
        props.put("group.id", groupID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.kafkaConsumer = new KafkaConsumer<String, String>(props);
        this.kafkaConsumer.subscribe(Arrays.asList(topicName));
    }

    public static void main(String[] args) {
        KafkaConsumerTest test1 = new KafkaConsumerTest("flume1", "groupA");
        Thread thread1 = new Thread(test1);
        thread1.start();
    }

    @Override
    public void run() {
        System.out.println("---------开始消费---------");
        try {
            for (; ; ) {
                consumerRecords = kafkaConsumer.poll(1000);//用1s时长拉取数据
                if (null != consumerRecords && consumerRecords.count() > 0) {
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        System.out.println("=======receive: key = " + record.key() + ", value = " + record.value() + " offset===" + record.offset());
                    }
                    continue;
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}