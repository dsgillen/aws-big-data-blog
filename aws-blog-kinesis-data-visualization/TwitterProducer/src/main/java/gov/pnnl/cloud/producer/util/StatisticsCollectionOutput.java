package gov.pnnl.cloud.producer.util;

import java.util.concurrent.atomic.AtomicBoolean;

public class StatisticsCollectionOutput  {
	
	private long period;
	private StatisticsCollection stats;
	private final AtomicBoolean running = new AtomicBoolean(false);
	
	public StatisticsCollectionOutput(long period, StatisticsCollection stats) {
		this.period = period;
		this.stats = stats;
	}
	
	public void start() {
		
		Runnable r = new Runnable() {

			@Override
			public void run() {
				while (running.get()) {
					stats.outStats();
					
					try {
						Thread.sleep(period);
					} catch (InterruptedException e) {
					}
				}
			}
			
		};
		
		Thread t = new Thread(r, "StatisticsCollectionOutput");
		running.set(true);

		t.start();
		
	}

	public void stop() {
		this.running.set(false);
	}
}
