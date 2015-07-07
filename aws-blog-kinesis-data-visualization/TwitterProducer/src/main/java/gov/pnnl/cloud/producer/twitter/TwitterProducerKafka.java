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

package gov.pnnl.cloud.producer.twitter;

import gov.pnnl.cloud.producer.kafka.KafkaProducerClient;
import gov.pnnl.cloud.producer.util.StatisticsCollection;
import gov.pnnl.cloud.producer.util.StatisticsCollection.Key;
import gov.pnnl.cloud.producer.util.StatisticsCollectionOutput;
import gov.pnnl.cloud.producer.util.StatsAwareLinkedBlockingQueue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducerKafka {
	
	private static final String TWIT_SECRET = "twitter.secret";
	private static final String TWIT_TOKEN = "twitter.token";
	private static final String TWIT_CONSUMER_SECRET = "twitter.consumerSecret";
	private static final String TWIT_CONSUMER_KEY = "twitter.consumerKey";
	private static final String HASHTAGS = "twitter.hashtags";
	private static final String TOPIC = "kafka.topic";
	private static final String DEFAULT_PROP_FILE_NAME = "AwsUserData";
	private static final String BROKER_LIST = "kafka.brokerlist";

	private static final Log LOG = LogFactory.getLog(TwitterProducerKafka.class);

	public static void main(String[] args) {
		TwitterProducerKafka client = new TwitterProducerKafka();
		String arg = args.length == 1 ? args[0] : null;
		client.run(arg);
	}

	public void run(String propFilePath) {
		loadFileProperties(propFilePath, DEFAULT_PROP_FILE_NAME);

		String consumerKey = System.getProperty(TWIT_CONSUMER_KEY);
		String consumerSecret = System.getProperty(TWIT_CONSUMER_SECRET);
		String token = System.getProperty(TWIT_TOKEN);
		String secret = System.getProperty(TWIT_SECRET);
		String topic = System.getProperty(TOPIC);
		String brokerList = System.getProperty(BROKER_LIST);
		
		StatisticsCollection stats = new StatisticsCollection();
		StatisticsCollectionOutput statsOut = new StatisticsCollectionOutput(30000, stats);
		statsOut.start();
		
		stats.setStatValue(Key.APPLICATION_START, System.currentTimeMillis());
		
		while (true) {
			/**
			 * Set up your blocking queues: Be sure to size these properly based
			 * on expected TPS of your stream
			 */
			BlockingQueue<String> msgQueue = new StatsAwareLinkedBlockingQueue(10000, stats);

			/**
			 * Declare the host you want to connect to, the endpoint, and
			 * authentication (basic auth or oauth)
			 */
			StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
			
			

			// These secrets should be read from a config file
			Authentication hosebirdAuth = new OAuth1(consumerKey,
					consumerSecret, token, secret);

			// create a new basic client - by default gzip is enabled
			Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
					.endpoint(endpoint).authentication(hosebirdAuth)
					.processor(new StringDelimitedProcessor(msgQueue)).build();

			client.connect();
			

			LOG.info("Got connection to Twitter");
			
			KafkaProducerClient producer = new KafkaProducerClient("", topic, 20, brokerList, stats);
			producer.connect();
			
			LOG.info("Got connection to Kafka");

			try {
				if (process(msgQueue, producer, stats)) {
					break;
				}

			} catch (Exception e) {
				// if we get here, our client has broken, throw away and
				// recreate
				e.printStackTrace();
			}
			// also, if we make it here, we have had a problem, so start again
			client.stop();
		}
	}

	private boolean process(BlockingQueue<String> msgQueue, KafkaProducerClient producer, StatisticsCollection stats) {

		int exceptionCount = 0;
		
		while (true) {
			try {
				// get message HBC from queue
				String msg = msgQueue.take();
				
				// use 'random' partition key
				String key = String.valueOf(System.currentTimeMillis());
				
				// send to Kinesis
				producer.post(key, msg);
				
				
			} catch (Exception e) {
				// didn't get record - move on to next\
				e.printStackTrace();
				
				
				if(++exceptionCount > 5) {
					// too many exceptions - lets reconnect and try again
					return false;
				}
			}
		}

	}
	
	private void loadFileProperties(String propFilePath, String defaultName) {
		try {
			
			if(propFilePath == null){
				String userHome = System.getProperty("user.home");
				propFilePath = userHome + "/" + defaultName + ".properties";
			}
			
			InputStream propFile = new FileInputStream(propFilePath);

			Properties p = new Properties(System.getProperties());
			p.load(propFile);
			System.setProperties(p);

			// got them from the file
			LOG.info("Got properties from file");

		} catch (FileNotFoundException e) {
			// file is not there...
		} catch (IOException e) {
			// can't read file
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}