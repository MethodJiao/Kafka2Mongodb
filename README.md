# Kafka2Mongodb
mongodb消费kafka消息  实现kafka消息同步入库mongodb

##1.KafkaUtil根据需要修改kafka配置
代码中默认是以GroupId为 `groupA`消费Topic为 `flume1`的kafka消息
```$xslt
        Properties props = new Properties();
        props.put("bootstrap.servers", connectConfig.getKafkaIp() + ":" + connectConfig.getKafkaPort());
        props.put("group.id", "groupA");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(Arrays.asList("flume1"));
```
##2.MongoUtil中根据需要修改mongdb配置
代码中连接mydb数据库
```$xslt
        ConnectConfig connectConfig = new ConnectConfig();
        //连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient
                (connectConfig.getMongodbIp(), Integer.parseInt(connectConfig.getMongodbPort()));

        //返回连接数据库对象
        return mongoClient.getDatabase("mydb");
```
##3.ConnectConfig中根据需要指定kafka和MongoDB的ip及端口
```$xslt
    private String kafkaIp = "192.168.50.201";
    private String kafkaPort = "9092";
    private String mongodbIp = "192.168.50.201";
    private String mongodbPort = "27017";
```
##4.Kafka2Mongodb中根据需要修改mongo连接的collection
代码中默认连接`netflows`
```$xslt
 MongoCollection<Document> mongoDatabaseCollection = mongoDatabase.getCollection("netflows");
```
##5.直接运行Kafka2Mongodb的main函数即可
---------开始消费---------
##6.本工程集成slf4j+log4j
如需更改请修改 log4j.properties内配置，log文件默认输出到运行路径下log文件夹内kafka2mongoDebug.log记录debug级别以上日志，kafka2mongoError.log记录error级别以上日志
##7.本工程已json方式生产kafka即可，下边是在生产者输入的内容，可以作为测试用
```aidl
{"employees":[{"firstName":"Bill","lastName":"Gates"},{"firstName":"George","lastName":"Bush"},{"firstName":"Thomas","lastName":"Carter"}]}
```