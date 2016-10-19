<h1>A simple DynamoDB data loader written in Java</h1>

<b>Clone the repository into your local directory and build using Maven:</b><br>
$mvn clean install<br>

<b>Usage:</b><br>
$java -jar ddbsampleloader-1.0.0.jar 1 10 1 0 2 400 12000 12000<br>

<b>Arguments:</b><br>
arg1: number of batches<br>
arg2: number of items per batch<br>
arg3: number of threads<br>
arg4: seed value for random number generation<br>
arg5: size of the DynamoDB partition/hash key (in KB)<br>
arg6: size of the DynamoDB payload/attribute (in KB)<br>
arg7: DynamoDB table Read Capacity Units (RCU)<br>
arg8: DynamoDB table Write Capacity Units (WCU)<br>

<b>Further information can be found here:</b><br>
http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html<br>
http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/batch-operation-document-api-java.html
