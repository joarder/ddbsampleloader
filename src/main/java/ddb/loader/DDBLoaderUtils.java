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

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * 
 * @author joarderk
 * A simple DynamoDB table class
 */

public class DDBLoaderUtils {
	
	// Creates a new DynamoDB table named "DDBSample"
	static void createTable() throws Exception {
		try {			
			CreateTableRequest create_req = new CreateTableRequest().withTableName(DDBSampleLoader.TBL_NAME)
				.withKeySchema(
						new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH))
                .withAttributeDefinitions(
                		new AttributeDefinition().withAttributeName("Id").withAttributeType(ScalarAttributeType.S))
                .withProvisionedThroughput(
                		new ProvisionedThroughput().withReadCapacityUnits(DDBSampleLoader.RCU).withWriteCapacityUnits(DDBSampleLoader.WCU));
			
			DDBSampleLoader.log.info("Creating a new table ...");
			TableUtils.createTableIfNotExists(DDBSampleLoader.ddb_client, create_req);
            
			DDBSampleLoader.log.info("Waiting for the table status to be 'ACTIVE' ...");
			TableUtils.waitUntilActive(DDBSampleLoader.ddb_client, DDBSampleLoader.TBL_NAME);
			
		}  catch(AmazonServiceException e) {
			DDBSampleLoader.log.error("Caught an AmazonServiceException ...");
            DDBSampleLoader.log.error("Error Message:    " + e.getMessage());
            DDBSampleLoader.log.error("HTTP Status Code: " + e.getStatusCode());
            DDBSampleLoader.log.error("AWS Error Code:   " + e.getErrorCode());
            DDBSampleLoader.log.error("Error Type:       " + e.getErrorType());
            DDBSampleLoader.log.error("Request ID:       " + e.getRequestId());
            
		} catch(AmazonClientException e) {
			DDBSampleLoader.log.error("Caught an AmazonClientException ...");
			DDBSampleLoader.log.error("Error Message:    " + e.getMessage());			
		}
	}
	
	// Puts a single item<id, payload> in the specified table
	static void putTableItem() {				
		Map<String, AttributeValue> item = getNewItem();
		PutItemRequest putItemRequest = new PutItemRequest(DDBSampleLoader.TBL_NAME, item);		
		DDBSampleLoader.ddb_client.putItem(putItemRequest);        
	}
	
	// Creates and returns a DynamoDB table item
	static Map<String, AttributeValue> getNewItem() {		
		DDBSample item = new DDBSample();		
        Map<String, AttributeValue> itemKV = new HashMap<String, AttributeValue>();
        
        itemKV.put("id", new AttributeValue(item.getId()));
        itemKV.put("payload", new AttributeValue(item.getPayload()));

        return itemKV;
    }
	
	// Creates and returns a payload of size @len in KB	
	static String createPayload(int len) {		
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len*1024; i++)
	        sb.append((char)(DDBSampleLoader.rand.nextInt(26) + 'a'));
		
		return sb.toString();
	} 
	
	// Checks whether the specified DynamoDB table "DDBSample" already exist or not
	static boolean isTableExist() {
	    try {	        
	        return TableStatus.ACTIVE.toString().equals(describeTable().getTableStatus());
	        
	    } catch (ResourceNotFoundException e) {
	    	DDBSampleLoader.log.error("Caught an ResourceNotFoundException ...");
			DDBSampleLoader.log.error("Error Message: " + e.getMessage());
	        
			return false;
	    }
	}
	
	// Processes and returns the DynamoDB table description
	static TableDescription describeTable() {
        DescribeTableRequest desc_req = new DescribeTableRequest().withTableName(DDBSampleLoader.TBL_NAME);
        TableDescription tbl_desc = DDBSampleLoader.ddb_client.describeTable(desc_req).getTable();  
        
        return tbl_desc;                
	}
	
	// Displays DynamoDB table description in the console
	static void showTable() {
		TableDescription tbl_desc = describeTable();
		showTableDescription(tbl_desc);
	}
	
	static void showTableDescription(TableDescription tbl_desc) {
		DDBSampleLoader.log.info("Table description: ");
		try {
			DDBSampleLoader.log.info((DDBSampleLoader.print_mapper).writerWithDefaultPrettyPrinter()
					.writeValueAsString(tbl_desc));
			
		} catch (JsonProcessingException e) {
			DDBSampleLoader.log.error("Caught an JsonProcessingException ...");
			DDBSampleLoader.log.error("Error Message: " + e.getMessage());
		}
	}
	
	// Counts the number of loaded items in DynamoDB table and table size in MB
	private volatile static int item_counter = 0;
    private volatile static double table_size = 0.0d;
    
    public static synchronized void increment(DDBSample item) {
        ++item_counter;
        
        try {
        	// DynamoDB measures item size in UTF-8 
			final byte[] utf8Bytes_id = item.getId().getBytes("UTF-8");				
			final byte[] utf8Bytes_payload = item.getPayload().getBytes("UTF-8");
			
			table_size += (utf8Bytes_id.length + utf8Bytes_payload.length);	// in bytes				
			
		} catch (UnsupportedEncodingException e) {			
			e.printStackTrace();
		}	        
    }

    public static synchronized void decrement() {
        --item_counter;
    }

    public static int getItemCount() {
        return item_counter;
    }
    
    public static double getTableSize() {
    	return (table_size/1024/1024/1024);
    }
}