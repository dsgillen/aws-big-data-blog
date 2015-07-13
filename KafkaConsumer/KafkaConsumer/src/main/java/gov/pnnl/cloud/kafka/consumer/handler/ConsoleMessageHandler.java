package gov.pnnl.cloud.kafka.consumer.handler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;




public class ConsoleMessageHandler implements KafkaMessageHandler {

	private static final Log LOG = LogFactory.getLog(ConsoleMessageHandler.class);


	@Override
	public void handleMessage(String message) {
		LOG.info(message);

	}

}
