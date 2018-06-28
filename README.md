# Who-To-Follow-Algorithm-for-Massive-Dataset-Mining


### [Link to Spark implementation](https://github.com/bemova/Who-To-Follow-Algorithm-for-Massive-Dataset-Mining/tree/master/spark-src). ###


### [Link to Hadoop implementation](https://github.com/bemova/Who-To-Follow-Algorithm-for-Massive-Dataset-Mining/tree/master/src). ###

input format is a hadoop or spark distributed file with following format:
```
1	 3 4 5     => this means user 1 follows user 3, 4, and 5.
2	 1 3 5
3	 1 2 4 5
4	 1 2 3 5
5	 3
```




Implemented algorithms show how we can recommend other user that might be intersting to follow for each user.

for this input example we have to have a recommendation like following fromat:
```
1	 2(2) 
2	 4(3)
3	
4	
5	 1(1) 2(1) 4(1)
```

