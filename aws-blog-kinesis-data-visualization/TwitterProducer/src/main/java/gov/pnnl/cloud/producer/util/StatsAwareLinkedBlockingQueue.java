package gov.pnnl.cloud.producer.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class StatsAwareLinkedBlockingQueue extends LinkedBlockingQueue<String> {
	

	private static final long serialVersionUID = 1L;
	
	StatisticsCollection stats;
	
	public StatsAwareLinkedBlockingQueue(int capacity, StatisticsCollection stats) {
		super(capacity);
		
		this.stats = stats;
	}
	
    public boolean offer(String message, long timeout, TimeUnit unit) throws InterruptedException {
    	stats.increment(StatisticsCollection.Key.TWITTER_MESSAGE_READ);
    	return super.offer(message, timeout, unit);
    }


}
