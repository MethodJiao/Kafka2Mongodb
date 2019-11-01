import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.KafkaUtil;
import util.MongoUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author Method.Jiao
 * @Date 2019/10/19 13:16
 */
class Kafka2Mongodb implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kafka2Mongodb.class);

    private Kafka2Mongodb() {
    }

    /**
     * 数据入库MongoDB
     */
    private void mongodbInsert(ConsumerRecords<String, String> consumerRecords) {
        //mongo db链接实例
        MongoDatabase mongoDatabase = MongoUtil.getConnect();
        //Mongodb database中 Collection名字
        MongoCollection<Document> mongoDatabaseCollection = mongoDatabase.getCollection("netflows");
        //入库数据集
        List<Document> kafkaDataList = new ArrayList<>();
        try {
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println("=======receive: key = " + record.key() + ", value = " + record.value() + " offset===" + record.offset());
                LOGGER.info("msg:[key:{},value:{}]", record.key(), record.value());
                Document document = Document.parse(record.value());
                kafkaDataList.add(document);
            }
        } catch (JsonParseException e) {
            e.printStackTrace();
            LOGGER.error("JsonParse Error Receive check msg:[msg:{}]", e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Error Receive msg:[msg:{}]", e.getMessage());
        }
        if (kafkaDataList.isEmpty()) {
            return;
        }
        try {
            //入库
            mongoDatabaseCollection.insertMany(kafkaDataList);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("MongoDB Error [msg:{}]", e.getMessage());
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
        try (KafkaConsumer<String, String> kafkaConsumer = KafkaUtil.getConnect()) {
            System.out.println("---------开始消费---------");
            for (; ; ) {
                //用1s时长拉取数据
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
                if (null != consumerRecords && consumerRecords.count() > 0) {
                    mongodbInsert(consumerRecords);
                    continue;
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.error("InterruptedException Error msg:[msg:{}]", e.getMessage());
        }
    }

    public static void main(String[] args) {
        //后台线程方式运行
        Kafka2Mongodb kafka2MongodbTask = new Kafka2Mongodb();
        Thread thread1 = new Thread(kafka2MongodbTask);
        thread1.start();
    }
}