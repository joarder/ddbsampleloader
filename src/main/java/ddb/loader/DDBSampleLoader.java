/*******************************************************************************
 * Copyright (c) 2016 Joarder Kamal.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

package main.java.ddb.loader;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A simple DynamoDB data loading or workload generation class
 * @author joarderk
 */

public class DDBSampleLoader {
	
	// Static variables
	static Logger log;
	static Random rand;
	static int seed;
	static AmazonDynamoDBClient ddb_client;
	static DynamoDBMapper ddb_mapper;	
	static ObjectMapper print_mapper;	
	static String TBL_NAME = "DDBSample";	
	static int KEY_SIZE; // in KB
	static int PAYLOAD_SIZE; // in KB	
	static long RCU;
	static long WCU;
	static DecimalFormat df3;
		
	// Initializations
	static void init() {
		log = LoggerFactory.getLogger(DDBSampleLoader.class);
		rand = new Random(seed);		
		ddb_client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		ddb_mapper = new DynamoDBMapper(ddb_client);
		print_mapper = new ObjectMapper();
		df3 = new DecimalFormat("#.###");
	}
	
	// Loads a sample DynamoDB table called "DDBSample"
	static void loadTable(int batch_count, int items_per_batch_count, int thread_count) 
			throws InterruptedException {
				
		ExecutorService threadPool = Executors.newFixedThreadPool(thread_count);
		
		for(int i = 0; i < thread_count; i++) {
			threadPool.submit(new Runnable() {
				public void run() {
		        	// Run in parallel
					for (int j = 0; j < batch_count; j++) {
						String thr_name = Thread.currentThread().getName();
						log.info("Loading started by thread-"+thr_name);
						
						log.info("\t >> Partially loading batch-"+(j+1)+" in the table ...");
					
						ArrayList<DDBSample> itemList = new ArrayList<DDBSample>();
						for(int k = 0; k < items_per_batch_count; k++) {
							DDBSample item = new DDBSample();
							itemList.add(item);
							DDBLoaderUtils.increment(item);
						}
		     				
						log.info("\t\t --> Adding "+items_per_batch_count+" items for batch-"+(j+1)+" in the table ...");
						ddb_mapper.batchSave(itemList);
					}
		        }
			});
		}
								 
		threadPool.shutdown();		 
		threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
	}
		
	// Main function
	public static void main(String[] args) throws Exception {
		int batch_count = Integer.parseInt(args[0]);
		int items_per_batch_count = Integer.parseInt(args[1]);
		int thread_count = Integer.parseInt(args[2]);
		
		seed = Integer.parseInt(args[3]);
		KEY_SIZE = Integer.parseInt(args[4]);
		PAYLOAD_SIZE = Integer.parseInt(args[5]);
		RCU = Integer.parseInt(args[6]);
		WCU = Integer.parseInt(args[7]);
		
		// Initialization
		init();		
		
		log.info("Default encoding: "+System.getProperty("file.encoding"));
		
		// Creates the table
		if(!DDBLoaderUtils.isTableExist())
			DDBLoaderUtils.createTable();		
		
		// Shows the table
		DDBLoaderUtils.showTable();
		
		// Loads the table
		long startTime = System.currentTimeMillis();
		loadTable(batch_count, items_per_batch_count, thread_count);
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		
		log.info("Total inserted item count: "+DDBLoaderUtils.getItemCount());
		log.info("Table size: "+df3.format(DDBLoaderUtils.getTableSize())+" GB");
		log.info("Loading time: "+df3.format((double)totalTime/1000/60)+" mins");
	}
}
