import Util.KafkaUtil;
import Util.MongoDBUtil;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author Method.Jiao
 * @Date 2019/10/19 13:16
 */
public class Kafka2Mongodb implements Runnable {

    public Kafka2Mongodb() {
    }
    //数据入库MongoDB
    public void MongodbInsert(ConsumerRecords<String, String> consumerRecords) {
        //mongo db链接实例
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //Mongodb database中 Collection名字
        MongoCollection<Document> mongoDBCollection = mongoDatabase.getCollection("netflows");
        //入库数据集
        List<Document> kafkaDataList = new ArrayList<>();
        for (ConsumerRecord<String, String> record : consumerRecords) {
            System.out.println("=======receive: key = " + record.key() + ", value = " + record.value() + " offset===" + record.offset());
            Document document = new Document("key", record.key())
                    .append("value", record.value());
            kafkaDataList.add(document);
        }
        if (kafkaDataList.isEmpty())
            return;
        mongoDBCollection.insertMany(kafkaDataList);//入库
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> kafkaConsumer = KafkaUtil.getConnect();

        System.out.println("---------开始消费---------");
        try {
            for (; ; ) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);//用1s时长拉取数据
                if (null != consumerRecords && consumerRecords.count() > 0) {
                    MongodbInsert(consumerRecords);
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

    public static void main(String[] args) {
        Kafka2Mongodb test1 = new Kafka2Mongodb();
        Thread thread1 = new Thread(test1);
        thread1.start();
    }
}