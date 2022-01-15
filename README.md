# SparkScalaExercises
General Spark Scala exercises
  
```
Final Project Description  
  
Assume we have a system to store information about warehouses and amounts of goods  
We have the following data with formats:  
- List of warehouse positions. Each record for a particular position is unique  
  Format: { positionId: Long, warehouse: String, product: String, eventTime: Timestamp }  
- List of amounts. Records for a particular position can be repeated. The latest record means the current amount.  
  Format: { positionId: Long, amount: BigDecimal, eventTime: Timestamp }  
<br/>Using Apache Spark, implement the following methods using DataFrame/Dataset API:  <br/><br/>
- Load required data from files, e.g. csv of json  
- Find the current amount for each position, warehouse, product  
- Find max, min, avg amounts for each warehouse and product  
  
Example inputs :  
  
list of warehouse positions (positionId, warehouse, product, eventTime)  
1, W-1, P-1, 1528463098  
2, W-1, P-2, 1528463100  
3, W-2, P-3, 1528463110  
4, W-2, P-4, 1528463111  
  
list of amounts (positionId, amount, eventTime)  
1, 10.00, 1528463098  # current amount  
1, 10.20, 1528463008  
2, 5.00,   1528463100   # current amount  
3, 4.90,   1528463100  
3, 5.50,   1528463111  # current amount  
3, 5.00,   1528463105  
4, 99.99, 1528463111  
4, 99.57, 1528463112   # current amount  
  
Outputs:  
current amount for each position, warehouse, product  
1, W-1, P-1, 10.20  
2, W-1, P-2, 5.00  
……  
max, min, avg amounts for each warehouse and product  
W-1, P-1, <max?>, <min?>, <avg?>  
…  
W-2, P-4, <max?>, <min?>, <avg?> 


Final Project V2 Description

You need to read Avro files and solve the following task: "A user posts a provocative message on Twitter. His subscribers do a retweet. Later, every subscriber's subscriber does retweet too."
Find the top ten users by a number of retweets in the first and second waves.
Entity's description.
Name: USER_DIR.
Description: Table contains first and last names.
USER_ID|FIRST_NAME|LAST_NAME
--------------------------------
1      |"Robert"    |"Smith"
--------------------------------
2      |"John"      |"Johnson"
--------------------------------
3      |"Alex"      |"Jones"
...
 

Name: MESSAGE_DIR
Description: Table contains text message.
MESSAGE_ID|TEXT
--------------------------------
11        |"text"
--------------------------------
12        |"text"
--------------------------------
13        |"text"
...
Name: MESSAGE
Description: Table contains information about posted messages.
USER_ID|MESSAGE_ID
--------------------------------
1      |11
--------------------------------
2      |12
--------------------------------
3      |13
...
Name: RETWEET

Description: Table contains information about retweets.
USER_ID|SUBSCRIBER_ID|MESSAGE_ID
--------------------------------
1      |2       |11 
--------------------------------
1      |3       |11 
--------------------------------
2      |5       |11 
--------------------------------
3      |7       |11
--------------------------------
7      |14       |11 
--------------------------------
5      |33       |11 
--------------------------------
2      |4       |12
--------------------------------
3      |8       |13
 

"Top one user" looks the following
USER_ID|FIRST_NAME|LAST_NAME|MESSAGE_ID|TEXT  |NUMBER_RETWEETS
------------------------------------------------------------
1      |"Robert"  |"Smith" |11        |"text"|4

Task acceptance criteria
Task solution should contain: 
1. Task solution with Dataframe or Dataset API.
2. Unit tests.
3. Example of Avro files.

