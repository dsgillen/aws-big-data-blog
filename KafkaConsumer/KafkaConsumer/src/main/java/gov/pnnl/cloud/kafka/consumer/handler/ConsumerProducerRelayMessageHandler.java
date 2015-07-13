package gov.pnnl.cloud.kafka.consumer.handler;

import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;




public class ConsumerProducerRelayMessageHandler implements KafkaMessageHandler {

	private static final Log LOG = LogFactory.getLog(ConsumerProducerRelayMessageHandler.class);

	private Producer<String, String> producer;
	String topic;

	public  ConsumerProducerRelayMessageHandler(Producer<String, String> producer, String topic) {
		this.producer = producer;
		this.topic = topic;
	}

	@Override
	public void handleMessage(String messageString) {

		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
		}
		KeyedMessage<String, String> message =  new KeyedMessage<String, String>(topic, UUID.randomUUID().toString(), messageString);
		producer.send(message);
	}

}
