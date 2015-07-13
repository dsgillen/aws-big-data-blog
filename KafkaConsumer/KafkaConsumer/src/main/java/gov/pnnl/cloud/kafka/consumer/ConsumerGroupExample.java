package gov.pnnl.cloud.kafka.consumer;


import gov.pnnl.cloud.kafka.consumer.handler.ConsoleMessageHandler;
import gov.pnnl.cloud.kafka.consumer.handler.CountMessageHandler;
import gov.pnnl.cloud.kafka.consumer.handler.KafkaMessageHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
 
public class ConsumerGroupExample {
    private final ConsumerConnector consumer;
    private final String topic;
    private KafkaMessageHandler handler;
    private  ExecutorService executor;
 
    public ConsumerGroupExample(String a_zookeeper, String a_groupId, String a_topic, KafkaMessageHandler handler) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
        this.handler = handler;
    }
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
   }
 
    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);
 
        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber, handler));
            threadNumber++;
        }
    }
 
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
 
        return new ConsumerConfig(props);
    }
 
    public static void main(String[] args) {
        String zooKeeper = args[0];
        String groupId = args[1];
        String topic = args[2];
        int threads = Integer.parseInt(args[3]);
        String handlerType = args[4];
        
        KafkaMessageHandler handler = null;
        if (handlerType.equals("console")) {
        	handler = new ConsoleMessageHandler();
        } else if (handlerType.equals("count10")) {
        	handler = new CountMessageHandler(topic, 10);
        }
 
        ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupId, topic, handler);
        example.run(threads);
 
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
 
        }
        //example.shutdown();
    }
}
