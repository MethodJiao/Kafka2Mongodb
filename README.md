# Kafka2Mongodb
mongodb消费kafka消息  实现kafka消息同步入库mongodb

1.根据需要修改kafka地址
```$xslt
private final String kafkaServer = "localhost:9092";
```
2.代码中默认是以GroupId为 `groupA`消费Topic为 `flume1`的kafka消息。
```$xslt
KafkaConsumerTest test1 = new KafkaConsumerTest("flume1", "groupA");
```