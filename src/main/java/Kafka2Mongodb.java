import org.bson.json.JsonParseException;
import util.KafkaUtil;
import util.MongoUtil;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Author Method.Jiao
 * @Date 2019/10/19 13:16
 */
public class Kafka2Mongodb implements Runnable {

    public Kafka2Mongodb() {
    }

    /**
     * 数据入库MongoDB
     */
    public void mongodbInsert(ConsumerRecords<String, String> consumerRecords) {
        //mongo db链接实例
        MongoDatabase mongoDatabase = MongoUtil.getConnect();
        //Mongodb database中 Collection名字
        MongoCollection<Document> mongoDatabaseCollection = mongoDatabase.getCollection("netflows");
        //入库数据集
        List<Document> kafkaDataList = new ArrayList<>();
        try {
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println("=======receive: key = " + record.key() + ", value = " + record.value() + " offset===" + record.offset());
                Document document = Document.parse(record.value());
                kafkaDataList.add(document);
            }
        } catch (JsonParseException e) {
            e.printStackTrace();
        }
        if (kafkaDataList.isEmpty()) {
            return;
        }
        //入库
        mongoDatabaseCollection.insertMany(kafkaDataList);
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> kafkaConsumer = KafkaUtil.getConnect();

        System.out.println("---------开始消费---------");
        try {
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
        } finally {
            kafkaConsumer.close();
        }
    }

    public static void main(String[] args) {
        //后台线程方式运行
        Kafka2Mongodb kafka2MongodbTask = new Kafka2Mongodb();
        Thread thread1 = new Thread(kafka2MongodbTask);
        thread1.start();
    }
}