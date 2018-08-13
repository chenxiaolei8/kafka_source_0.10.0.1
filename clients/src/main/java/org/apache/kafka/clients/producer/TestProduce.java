package org.apache.kafka.clients.producer;

import java.util.Properties;

/**
 * @Author chenxiaolei3
 * @Mail chenxiaolei3@jd.com
 * @Description
 * @DATE Created in 17:45 2017/8/27
 * @Modify by
 */
public class TestProduce {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.177.79:9092,192.168.177.80:9092");
//        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
//        props.put("interceptor.classes", "org.apache.kafka.clients.producer.ProducerInterceptorDemo");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("interceptor.classes", "org.apache.kafka.clients.producer.ProduceInterceptorMyDemo");
     new Thread(new Produces(props)).start();
//     new Thread(new Produces(props)).start();
//     new Thread(new Produces(props)).start();
    }
}
class Produces implements Runnable{
private Properties properties;

    public Produces(Properties properties) {
        this.properties = properties;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void run() {
        Producer<Integer, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 3; i++)
            producer.send(new ProducerRecord<Integer, String>("chen4", i, "chenxiaolei" + i),
                    new DemoCallBack(System.currentTimeMillis(), 1, "1232333")
            );
        producer.close();
    }
}
/**
 * 回调类
 */
class DemoCallBack implements Callback {
    // 消息开始处理时间
    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            // RecordMetadata中包含了分区信息、Offset信息等
            System.out.println(Thread.currentThread().getName() + " message(" + key + ", " + message + ") sent to partition(" +
                    metadata.partition() + "), " + "offset(" + metadata.offset() + ") in " +
                    elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}