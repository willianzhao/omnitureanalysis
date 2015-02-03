User Transaction Stitching
========================

The Map-Reduce program is to analyze transaction attributes

## Compile and packing

 * initial get the required libs
 __make sure the maven version is 3.0.4 or 3.0.5__
 __mvn -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true clean install__

 * first compile the public-lib project
    __cd projects/public__
    __mvn clean install__

The jar output is __target/public-1.0.0.jar__

 * then compile x-platform-txn-attribution project
    __cd projects/x-platform-txn-attribution__
    __mvn clean install__

The jar output is __target/x-platform-txn-attribution-1.0.0.jar__

## Standalone Execute
### Trasnaction Stitching ###
 __hadoop jar ./x-platform-txn-attribution-1.0.0.jar -D mapred.child.java.opts=-Xmx2g -D mapred.map.tasks=500 -D mapred.reduce.tasks=74 -m checkoutbehavior -s 20140101 -e 20140304 –n 31__
 
 - -D mapred.child.java.opts :    Java opts for the task tracker child processes.
 - -D mapred.map.tasks=300 : The default number of map tasks per job. Ignored when mapred.job.tracker is "local".
 - -D mapred.reduce.tasks=28 : The default number of reduce tasks per job. Typically set to 99% of the cluster's reduce capacity, so that if a node fails the reduces can still be executed in a single wave. Ignored when mapred.job.tracker is "local".
 - -m : means we will run the checkout data analysis (task2).
    * __checkoutbehavior__ : perform task2 data analysis using omniture data as source
    * __createeventmapfile__ : create event mapfile only
    * __createusermapfile__ : create user mapfile only
    * __createstubtransfeed__ : create transaction data only
    * __stubtransbehavior__ : perform task2 data analysis using stub_trans data as source
    * __unittest__ : run the unit test

 - -s : means the start date of transactions. If we want to analyze the Q1 transaction, we should provide 20130101 for ‘-s’
 - -e : means the end date of transactions. Then for Q1, it’s 20130331. 
 - -n : the number of days look back for mobile browing records. If this parameter is not provided, the look back days will be default to 0.
    
## Data load strategy

According to the start and end date of transactions, MR program will automatically load :

1.	Mweb/ipad/mobileApps HDFS files from the prior N (30) days from the start date of transactions up to the end date of transactions. E.g. if we study the Q1 transactions, then 4 months of mweb/ipad/mobileapp data from Dec 2013 – Mar 2014 will be loaded

2.	All the events HDFS files will be extracted with <eventID, genreID> and construct a mapfile 


## Output location

The output root folder in HDFS is fixed to /data/processed/analyzer . This can be changed in the configure file settings.

## Data Format

The header for checkout analysis metrics is (delimiter ‘ ~ ‘)

(To be Continued)