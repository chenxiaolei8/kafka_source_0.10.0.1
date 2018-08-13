package org.apache.kafka.clients.consumer.internals;

/**
 * @Author chenxiaolei3
 * @Mail chenxiaolei3@jd.com
 * @Description
 * @DATE Created in 16:07 2018/5/8
 * @Modify by
 */
public class ChenTest {
    public static void main(String[] args) {
        Target mAdapter = new Adapter();
        mAdapter.Request();
    }
}

interface Target {
    public void Request();
}

class Adaptee {
    public void SpecificRequest() {
        System.out.println(Thread.currentThread().getName() + " => SpecificRequest");
    }
}

class Adapter extends Adaptee implements Target {

    @Override
    public void Request() {
        this.SpecificRequest();
    }
}
