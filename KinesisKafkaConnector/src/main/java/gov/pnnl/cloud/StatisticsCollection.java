package gov.pnnl.cloud;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class StatisticsCollection {

	public enum Key { KINESIS_MESSAGE_READ, KAFKA_MESSAGE_PUT, APPLICATION_START, APPLICATION_END, KAFKA_WRITE_ERROR};
	
	private Map<Key, AtomicLong> stats;
	
	public StatisticsCollection() {
		stats = new HashMap<Key, AtomicLong>();
		
		for (Key key : Key.values()) {
			stats.put(key,  new AtomicLong(0L));
		}
	}
	
	public void outStats() {
		System.out.println("***********\n" + new Date());
		for (Key key : Key.values()) {
			System.out.println(key + ":" + stats.get(key));
		}
	}
	
	public void setStatValue(Key key, Long value) {
		stats.get(key).set(value);
	}
	
	public void increment(Key key) {
		stats.get(key).incrementAndGet();
	}
	
	public long getStatValue(Key key) {
		return stats.get(key).get();
	}
}
