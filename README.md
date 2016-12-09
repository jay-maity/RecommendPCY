# PCY algorithm to construct a recommendation model

It uses PCY algorithm to generate frequent item pairs and recomment product from there

### Installation

It requires pyspark, [pyspark-cassandra](https://github.com/TargetHolding/pyspark-cassandra) driver to run.
Also needs a [cassandra cluster](http://cassandra.apache.org/download/) to store data and intermediate calculations 
Install the dependencies run the item_sets.py.

Execute SQL in cassandra cluster from /sql folder

### Execution to generate item sets
There are two ways to execute the code
1. Using Cassandra for intermediate calculation
2. Using broadcast to the cluster (It works well with 1million items and average basket size of 10)

```sh
$ ${SPARK_HOME}/bin/spark-submit --master=local --packages TargetHolding/pyspark-cassandra:0.3.5 item_sets.py /inputpath /path <support_thresold> <broadcast=0,1>
```

### Web server setup to test
Install Flask python dependencies 

```sh
$ sudo pip install flask
```
Then move to /web folder and run the following commad to start the web server
```sh
$ python app.py
```
By default it runs on 0.0.0.0:5000, you can change the post and server by modifying last line of the code for app.py

### How to search for recommended items
Open the link 0.0.0.0:5000 in web browser
In the UI, search for item with product id 

License
----
MIT
