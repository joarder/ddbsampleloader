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

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

/**
 * 
 * @author joarderk
 * A simple DynamoDB table class with only a partition key and a single attribute
 */

@DynamoDBTable(tableName = "DDBSample")
public class DDBSample {
	private String id;
	private String payload;
	
	public DDBSample() {
		this.id = DDBLoaderUtils.createPayload(DDBSampleLoader.KEY_SIZE);		
		this.payload = DDBLoaderUtils.createPayload(DDBSampleLoader.PAYLOAD_SIZE);		
	}
	
	@DynamoDBHashKey(attributeName="Id")
	public String getId() {
		return id;
	}
	
	@DynamoDBAttribute(attributeName="Payload")
	public void setId(String id) {
		this.id = id;
	}
	
	public String getPayload() {
		return payload;
	}
	
	public void setPayload(String payload) {
		this.payload = payload;
	}
}