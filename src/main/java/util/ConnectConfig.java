package util;

/**配置类
 * @Author Method.Jiao
 * @Date 2019/10/19 13:16
 */
public class ConnectConfig {
    public String getKafkaIp() {
        return kafkaIp;
    }

    public String getKafkaPort() {
        return kafkaPort;
    }

    public String getMongodbIp() {
        return mongodbIp;
    }

    public String getMongodbPort() {
        return mongodbPort;
    }

    private String kafkaIp = "localhost";
    private String kafkaPort = "9092";
    private String mongodbIp = "localhost";
    private String mongodbPort = "27017";

}
