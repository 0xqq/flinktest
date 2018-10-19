package flink.streaming.practice.kafka;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Author:chenchen
 * @Description:
 * @Date:2018/9/19
 * @Project:sparktest
 * @Package:enn.enndigit.kafka
 */

public class JavaProducer extends Thread
{
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public JavaProducer(String topic)
    {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "10.39.43.52:9092,10.39.43.55:9092,10.39.43.56:9092");
        // Use random partitioner. Don't need the key type. Just set it to Integer.
        // The message is of type String.
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    public void run() {
        int messageNo = 1;
        while(true)
        {
            try {
                String messageStr = new String("{\"metric\":\"UES.P\",\"tags\":{\"equipID\":\"AA40205\",\"equipMK\":\"ACLN\",\"staId\":\"CA08ES05\"},\"timestamp\":1534918223869,\"value\":" + messageNo + "}");
                producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
                Thread.sleep(3000);
                System.out.println(messageStr);
                messageNo++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        JavaProducer producerThread = new JavaProducer("flinktest03");
        producerThread.start();
    }

}
