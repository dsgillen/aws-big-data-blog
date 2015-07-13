package gov.pnnl.cloud.kafka.consumer.handler;

public interface KafkaMessageHandler {

	public void handleMessage(String message);
}
