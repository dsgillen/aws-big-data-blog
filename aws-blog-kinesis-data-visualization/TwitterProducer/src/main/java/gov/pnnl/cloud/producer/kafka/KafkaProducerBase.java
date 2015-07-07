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

package gov.pnnl.cloud.producer.kafka;

import gov.pnnl.cloud.producer.util.Event;
import gov.pnnl.cloud.producer.util.StatisticsCollection;
import gov.pnnl.cloud.producer.util.StatisticsCollection.Key;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import kafka.producer.KeyedMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable class responsible for sending items on the queue to Kinesis
 * @author corbetn
 *
 */
public class KafkaProducerBase implements Runnable {
	   private static final Log LOG = LogFactory.getLog(KafkaProducerBase.class);

	/**
	 * Reference to the queue
	 */
	private final BlockingQueue<Event> eventsQueue;
	
	
	/**
	 * Reference to the Amazon Kinesis Client
	 */
	private final kafka.javaapi.producer.Producer<String, String> producer;
	
	
	/**
	 * The topic name that we are sending to
	 */
	private final String topic;
	


	private StatisticsCollection stats;
	
	private final static Logger logger = LoggerFactory
			.getLogger(KafkaProducerBase.class);


	/**
	 * @param eventsQueue The queue that holds the records to send to Kinesis
	 * @param kinesisClient Reference to the Kinesis client
	 * @param streamName The stream name to send items to
	 */
	public KafkaProducerBase(BlockingQueue<Event> eventsQueue,
			kafka.javaapi.producer.Producer<String, String> producer, String topic, StatisticsCollection stats) {
		this.eventsQueue = eventsQueue;
		this.producer = producer;
		this.topic = topic;
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

				KeyedMessage<String, String> message = null;
				List< KeyedMessage<String, String>> messageList = new ArrayList< KeyedMessage<String, String>>();

				// add the message 150 times, to add volume!
				for (int i=0; i<150; i++) {
					message = new KeyedMessage<String, String>(topic, new String(event.getData().array()));
					stats.increment(Key.KAFKA_MESSAGE_GENERATE);

					messageList.add(message);
				}

				try {
					producer.send(messageList);
					stats.increment(Key.KAFKA_MESSAGE_PUT);

				} catch (Throwable t) {
					LOG.warn("Caught throwable while processing batch of " + messageList.size() + " records", t);
				}



			} catch (Exception e) {
				// didn't get record - move on to next\
				e.printStackTrace();	

				stats.increment(Key.KAFKA_PUT_ERROR);
			}
		}

	}
}
