package gov.pnnl.cloud.kafka.consumer;

import gov.pnnl.cloud.kafka.consumer.handler.KafkaMessageHandler;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
 
public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private KafkaMessageHandler handler;
 
    public ConsumerTest(KafkaStream a_stream, int a_threadNumber, KafkaMessageHandler handler) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        this.handler = handler;
    }
 
    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            handler.handleMessage(new String(it.next().message()));
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
