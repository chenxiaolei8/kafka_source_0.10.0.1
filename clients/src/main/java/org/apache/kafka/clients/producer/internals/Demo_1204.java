package org.apache.kafka.clients.producer.internals;

import java.nio.channels.SelectionKey;

/**
 * @Author chenxiaolei3
 * @Mail chenxiaolei3@jd.com
 * @Description
 * @DATE Created in 17:08 2017/12/4
 * @Modify by
 */
public class Demo_1204 {
    //do something
    public static void main(String[] args) {
        System.out.println(2 << 3);
        System.out.println(1 << 2);
        System.out.println(1 << 3);
        System.out.println(SelectionKey.OP_READ & (SelectionKey.OP_READ | SelectionKey.OP_WRITE));
        System.out.println(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        System.out.println(SelectionKey.OP_CONNECT );
        System.out.println(13| 15);
    }
}
