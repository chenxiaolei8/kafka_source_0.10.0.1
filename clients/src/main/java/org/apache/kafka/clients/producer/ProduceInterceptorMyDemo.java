package org.apache.kafka.clients.producer;

import java.util.Map;

/**
 * @Author chenxiaolei3
 * @Mail chenxiaolei3@jd.com
 * @Description
 * @DATE Created in 11:48 2017/8/14
 * @Modify by
 */
public class ProduceInterceptorMyDemo implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        if ("chen".equals(record.key())) {
            return null;
        }
        return record;
    }
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null)
            System.out.println(metadata.toString());
        System.out.println("metadata is null!");
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
