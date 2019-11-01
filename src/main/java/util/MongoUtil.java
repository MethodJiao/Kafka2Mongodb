package util;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.util.Collections;

/**
 * @Author Method.Jiao
 * @Date 2019/10/19 13:36
 */
public class MongoUtil {
    /**
     * 不通过认证获取连接数据库对象
     */
    public static MongoDatabase getConnect() {
        ConnectConfig connectConfig = new ConnectConfig();
        //连接到 mongodb 服务
        MongoClient mongoClient = MongoClients.create("mongodb://" + connectConfig.getMongodbIp() + ":" + connectConfig.getMongodbPort());
        //返回连接数据库对象
        return mongoClient.getDatabase("mydb");
    }

    /**
     * 需要密码认证方式连接
     */
    public static MongoDatabase getConnect2() {

        //通过连接认证获取MongoDB连接
        String user = "";
        String database = "";
        char[] password = {'p', 'a', 's', 's'};

        MongoCredential credential = MongoCredential.createCredential(user, database, password);

        MongoClientSettings settings = MongoClientSettings.builder()
                .credential(credential)
                .applyToSslSettings(builder -> builder.enabled(true))
                .applyToClusterSettings(builder ->
                        builder.hosts(Collections.singletonList(new ServerAddress("host1", 27017))))
                .build();

        MongoClient mongoClient = MongoClients.create(settings);
        //返回连接数据库对象
        return mongoClient.getDatabase("mydb");
    }
}
