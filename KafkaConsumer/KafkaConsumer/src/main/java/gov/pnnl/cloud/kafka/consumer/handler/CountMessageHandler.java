package gov.pnnl.cloud.kafka.consumer.handler;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class CountMessageHandler implements KafkaMessageHandler {
	private static final Log LOG = LogFactory.getLog(CountMessageHandler.class);

	private final AtomicLong count = new AtomicLong(0);
	private  Timer timer = null;
	
	public CountMessageHandler(final String topic, int seconds) {
		
		timer = new Timer(true);
		timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				LOG.info(topic + ": " + count.get());
				
			}
			
		}, 0, seconds * 1000);
	}
	


	@Override
	public void handleMessage(String message) {
		count.incrementAndGet();
	}
	

}
