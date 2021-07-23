package com.intel.streaming_benchmark.utils;

import com.alibaba.fastjson.JSONObject;
import com.intel.streaming_benchmark.ClickProducer;
import com.intel.streaming_benchmark.common.ConfigLoader;
import com.intel.streaming_benchmark.common.StreamBenchConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class GetProducer extends Thread {

    private String topic;
    private Long time;
    private ConfigLoader cl;

    private static final char[] HEX_CHARS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public GetProducer(String topic, Long time, ConfigLoader cl) {

        super();
        this.topic = topic;
        this.time = time;
        this.cl = cl;
    }

    @Override
    public void run() {

        System.out.println(Thread.currentThread().getName() + "=======");

        if (topic.equals("Shopping_record")) {

            datagenTopic1(cl);
        } else if (topic.equals("Real_time_Advertising")) {
            datagenTopic2(cl);
        } else if (topic.equals("User_visit_session_record")) {
            new ClickProducer(time, cl).run();
        } else {
            System.out.println("No such scene!");
        }

    }

    private KafkaProducer createProducer(ConfigLoader cl) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common" +
                ".serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common" +
                ".serialization.ByteArraySerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                cl.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST));
        return new KafkaProducer<>(properties);
    }

    private void datagenTopic1(ConfigLoader cl) {

        String[] commodities = {"fig", "pork", "water", "Duck", "tea", "beer", "sandwich", "ham",
                "pickle", "milk", "bag", "book", "desk", "sweet", "food", "disk", "pen", "shoe",
                "animal", "phone", "paper", "cup", "light", "glass", "power", "GameBoy",
                "chopsticks"};
        Random random = new Random();
        KafkaProducer producer = createProducer(cl);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long start = System.currentTimeMillis();
        Boolean flag = true;
        Long count = 0L;
        Long totalLength = 0L;
        String threadName = Thread.currentThread().getName();

        byte[] bytes = new byte[2048];
        random.nextBytes(bytes);
        String payload = byteToHexString(bytes, 0, bytes.length);

        try {

            InetAddress address = InetAddress.getLocalHost();
            String hostName = address.getHostName().toString();
            while (flag) {
                byte[] message =
                        (hostName + "_" + count + "_" + threadName + "," + commodities[random.nextInt(commodities.length)] + "," + System.currentTimeMillis() + "," + payload)
                                .getBytes();
                producer.send(new ProducerRecord("shopping", message));
                count = count + 1;
                totalLength = totalLength + message.length;
                if ((System.currentTimeMillis() - start) > time * 1000) {
                    flag = false;
                    System.out.println(count + "  " + totalLength);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }

    private void datagenTopic2(ConfigLoader cl) {
        Long count = 0L;
        Long totalLength = 0L;
        Long impCount = 0L;
        Long clickCount = 0L;
        Long dauCount = 0L;

        KafkaProducer producer = createProducer(cl);
        long start = System.currentTimeMillis();
        Boolean flag = true;
        Random random = new Random();
        String strategy_all[] = {"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"};
        //t1:strategy1, t2:strategy2,,, t6:strategy6
        String site_all[] = {"1", "2", "3", "4", "5", "6", "7", "8"};//1:baidu media,2:toutiao
        // media,3: weibo media
        String pos_id_all[] = {"a", "b", "c", "d", "e", "f", "g"};//a:ad space,b:ad space,c:ad space
        String poi_id_all[] = {"1001", "1002", "1003", "1004", "1005", "1006", "1007"};//1001:ad
        // material,1002:ad material,1003:ad material
        String cost_all[] = {"0.01", "0.02", "0.03", "0.10", "0.08", "0.05", "0.20"};//cost
//        String device_id_all[] ={"aaaaa","bbbbb","ccccc","ddddd","eeeee","fffff","ggggg"};//device

        int device_count = 10;
        String device_id_all[] = new String[device_count];
        for (int i = 0; i < device_count; i++) {
            device_id_all[i] = UUID.randomUUID().toString();
        }

        try {
            String threadName = Thread.currentThread().getName();
            InetAddress address = InetAddress.getLocalHost();
            String hostName = address.getHostName().toString();
            while (flag) {
                JSONObject imp = new JSONObject();
                imp.put("imp_time", Long.valueOf(System.currentTimeMillis()));
                imp.put("strategy", strategy_all[random.nextInt(strategy_all.length - 1)]);
                imp.put("site", pos_id_all[random.nextInt(site_all.length - 1)]);
                imp.put("pos_id", strategy_all[random.nextInt(pos_id_all.length - 1)]);
                imp.put("poi_id", poi_id_all[random.nextInt(poi_id_all.length - 1)]);
                imp.put("cost", cost_all[random.nextInt(cost_all.length - 1)]);
                imp.put("device_id", device_id_all[random.nextInt(device_id_all.length - 1)]);
                imp.put("sessionId", hostName + "_" + count + "_" + threadName);
                //send exposure log
                byte[] imp_message = imp.toJSONString().getBytes();
                producer.send(new ProducerRecord("imp", imp_message));

                count++;
                impCount++;
                totalLength = totalLength + imp_message.length;

                if (random.nextInt(4) == 1) {//the probablity of triggerring Click
                    byte[] bytes = new byte[2048];
                    random.nextBytes(bytes);
                    String payload = byteToHexString(bytes, 0, bytes.length);

                    JSONObject click = imp;
                    click.remove("imp_time");
                    click.remove("cost");
                    click.put("click_time", Long.valueOf(System.currentTimeMillis()));
                    click.put("payload", payload);
                    byte[] click_message = click.toJSONString().getBytes();
                    producer.send(new ProducerRecord("click", click_message));

                    count++;
                    clickCount++;
                    totalLength = totalLength + click_message.length;

                    if (random.nextInt(2) == 1) {//dau time,?50
                        JSONObject dau = new JSONObject();
                        dau.put("dau_time", Long.valueOf(System.currentTimeMillis()));
                        dau.put("device_id", click.get("device_id").toString());
                        dau.put("sessionId", click.get("sessionId"));
                        byte[] dau_message = dau.toJSONString().getBytes();
                        producer.send(new ProducerRecord("dau", dau_message));

                        count++;
                        dauCount++;
                        totalLength = totalLength + dau_message.length;
                    }
                }
                if ((System.currentTimeMillis() - start) > time * 1000) {
                    flag = false;
                    System.out.println(count + "  " + totalLength);
                    System.out.println("imp count:  " + impCount);
                    System.out.println("click count:  " + clickCount);
                    System.out.println("dau count:  " + dauCount);
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static String byteToHexString(byte[] bytes, int start, int end) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes == null");
        } else {
            int length = end - start;
            char[] out = new char[length * 2];
            int i = start;

            for (int var6 = 0; i < end; ++i) {
                out[var6++] = HEX_CHARS[(240 & bytes[i]) >>> 4];
                out[var6++] = HEX_CHARS[15 & bytes[i]];
            }

            return new String(out);
        }
    }
}
