/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package gov.pnnl.cloud.producer.kinesis;

import gov.pnnl.cloud.producer.util.StatisticsCollection;
import gov.pnnl.cloud.producer.util.StatisticsCollection.Key;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

/**
 * Runnable class responsible for sending items on the queue to Kinesis
 * @author corbetn
 *
 */
public class ProducerBase implements Runnable {

	/**
	 * Reference to the queue
	 */
	private final BlockingQueue<Event> eventsQueue;
	
	
	/**
	 * Reference to the Amazon Kinesis Client
	 */
	private final AmazonKinesis kinesisClient;
	
	
	/**
	 * The stream name that we are sending to
	 */
	private final String streamName;
	


	private StatisticsCollection stats;
	
	private final static Logger logger = LoggerFactory
			.getLogger(ProducerBase.class);


	/**
	 * @param eventsQueue The queue that holds the records to send to Kinesis
	 * @param kinesisClient Reference to the Kinesis client
	 * @param streamName The stream name to send items to
	 */
	public ProducerBase(BlockingQueue<Event> eventsQueue,
			AmazonKinesis kinesisClient, String streamName, StatisticsCollection stats) {
		this.eventsQueue = eventsQueue;
		this.kinesisClient = kinesisClient;
		this.streamName = streamName;
		this.stats = stats;

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {

		while (true) {
			try {
				// get message from queue - blocking so code will wait here for work to do
				Event event = eventsQueue.take();

				List<PutRecordsRequestEntry> puts = new ArrayList<PutRecordsRequestEntry>();

				// bump up the volume!
				for (int i=0; i<150; i++) {

					PutRecordsRequestEntry put = new PutRecordsRequestEntry();

					put.setData(event.getData());
					put.setPartitionKey(String.valueOf(System.currentTimeMillis()));
					puts.add(put);
					
					stats.increment(Key.KINESIS_MESSAGE_GENERATED);

				}

				System.out.println(stats.getStatValue(Key.KINESIS_MESSAGE_WRITTEN));

				synchronized(stats) {
					PutRecordsRequest put = new PutRecordsRequest();
					put.setRecords(puts);
					put.setStreamName(this.streamName);

					PutRecordsResult result = kinesisClient.putRecords(put);
					//logger.info(result.getSequenceNumber() + ": {}", this);	
					stats.increment(Key.KINESIS_MESSAGE_WRITTEN);

					if (stats.getStatValue(Key.KINESIS_MESSAGE_WRITTEN) > 10000) {
						stats.outStats();
						System.exit(0);
					} else {
						System.out.println(stats.getStatValue(Key.KINESIS_MESSAGE_WRITTEN));
					}
				}


			} catch (Exception e) {
				// didn't get record - move on to next\
				e.printStackTrace();	
				
				stats.increment(Key.KINESIS_WRITE_ERROR);
			}
		}

	}
}
