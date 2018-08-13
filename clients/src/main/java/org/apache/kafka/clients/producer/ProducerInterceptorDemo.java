package org.apache.kafka.clients.producer;

import java.util.Map;

/**
 * @Author chenxiaolei3
 * @Mail chenxiaolei3@jd.com
 * @Description
 * @DATE Created in 15:03 2017/11/27
 * @Modify by
 */
public class ProducerInterceptorDemo implements ProducerInterceptor {
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        System.out.println("onSend" + record.topic());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.out.println("返回结果  ==>  " + metadata.offset());
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
    //do something
}
