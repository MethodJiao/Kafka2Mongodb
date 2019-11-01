package util;

/**配置类
 * @Author Method.Jiao
 * @Date 2019/10/19 13:16
 */
class ConnectConfig {
    public String getKafkaIp() {
        return "localhost";
    }

    public String getKafkaPort() {
        return "9092";
    }

    public String getMongodbIp() {
        return "localhost";
    }

    public String getMongodbPort() {
        return "27017";
    }

}
