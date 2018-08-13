package org.apache.kafka.clients.producer.internals;

/**
 * @Author chenxiaolei3
 * @Mail chenxiaolei3@jd.com
 * @Description
 * @DATE Created in 14:16 2018/5/23
 * @Modify by
 */
public class Test {
    public static void main(String[] args) {
        String[][] s = new String[4][];
        s[1] = new String[]{"", "", "", "", ""};
        s[3] = new String[]{"", "", "", "", ""};
        s[2] = new String[]{"", "", "", "", ""};
        s[0] = new String[]{"", "", "", "", "",""};
        for (String[] strings : s) {
            System.out.println(strings.length);
        }
    }
}
