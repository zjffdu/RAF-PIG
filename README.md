# RAF-PIG (Rich API for [Pig](http://pig.apache.org)) #

## To use ##
1. git clone
2. mvn package
3. check out target/dw.pig-${version}.jar
4. check out src/examples for usage of RAF-PIG

## License ##
Apache licensed.

## The sugars RAF-PIG provides ##

### Asynchronously execute pig script ###

The original way to run pig script in embeded mode is create a PigServer, then call PigServer.register() to run pig script(this api is blocking method). 
If your script has dump or store statement, you have to wait for a long time until the mapreduce job completes. Or you can call PigServer.openIterator() which is also blocking method.
Actually pig client has not so much thing to be involved in the execution of pig script. You can do other thing on client when you run Pig script on hadoop cluster. 
To improve the efficiency, RAF-PIG provides an asynchronous way to run pig script. The following is an simple example,
Pig script
<code><pre>
	a = load '$input' as (f1:chararray,f2:int,f3:int);
	b = foreach a generate f1,f2;
	c = group b by f1;
	d = foreach c generate group,SUM(b.f2);
	store d into '$output';
</pre></code>
Java code
<code><pre>
	PigConfiguration conf = new PigConfiguration();
	PigSession session = new PigSession(conf);
	try {
	    final String input = "src/examples/data/input/example.txt";
	    final String output = "src/examples/data/output_1";
	    PigJob job = PigJobs
	            .newPigJobFromFile(
	                    new File("src/examples/scripts/example_1.pig"))
	            .setJobName("example_1").setParameter("input", input)
	            .setParameter("output", output)
	            .setListener(new PigJobListenerAdapter() {
	                @Override
	                public void beforeStart(PigJob job) throws Exception {
	                    // delete the destination folder before execution,
	                    // this should be useful when you do local test
	                    FileSystem fs = FileSystem.get(new Configuration());
	                    fs.delete(new Path(output));
	                }
	
	                @Override
	                public void onSuccess(PigJob job) throws Exception {
	                    // print success message
	                    System.out.println("Exeucte pig job sucessfully:\n"
	                            + job.getScript());
	                }
	            });
	
	    PigJobFuture future = session.submitPigJob(job);  // this method return immediately
	    future.await();  // this method block until this PigJob completes
	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	    session.close();
	}
</pre></code>

Here, we introduce 4 important classes for RAF-PIG. 

1.	PigConfiguration encapsulate common property for your PigJobs, currently support four types of Options.
	* ExecType 	(Local or MapReduce)
	* PoolSize (The default value is 1 because currently Pig do not support concurrently execution pig script in one JVM)
	* UDFJar (Help you register jars automatically, you do not need to register jar in pig script)
	* UDFPackage ( Import UDF package, you do not need to write the full-qualified class name in pig script)
	
2.	PigSession is a pig script submitter you can consider it as a session between you and Hadoop cluster, and you can submit PigJob through this class. 
3.	PigJob is the basic interface which wrap a concrete pig script and any payload around PigJob. You can setJobName,setPriority,setParameter and setScriptSource through interface PigJob. 
4.	PigJobFuture is a object help you track the progress of this PigJob


### Add hook before and after pig script execution ###

Since we provide an asynchronous way to run pig script, then how do you get the result. PigJob has a method setPigJobListener which you allow you hook method before and after the life-cycle of pig script execution.
Interface PigJobListener has three methods beforeStart, onSuccess, onFailure. It is very easy to guess the intention of these methods. And we provide an implementation PigJobListernAdapter for easy extension.


### Provide different ways to get the output of PigJob ###

First I'd like to classify pig script as following two types 
	* Having dump or store statement, this kind of script would generate mapreduce job before the calling of method PigJobListener.onSucess(PigJob job).
	* Without dump or store statement, this kind of pig script won't generate mapreduce job until you call PigJob.getOutput(alias), most of time you should call PigJob.getOutput(alias) in PigJobListener.onSucess(PigJob job).

Interface PigJob provides two kinds of way to get output. 
	* Get output from the destination source, such as PigJob.getOutput(Path path, String loadFuncClass), this is often used for pig script with store statement.
	* Get output from the alias, such as PigJob.getOutput(String alias), this is often used for pig script without store statement.


### Provide extract pattern for convert pig data structure to your domain data structure ###

The output of pig script is always tuples which is less semantics for application. You may want to convert pig data structure to your domain data structure. RAF-PIG provide two interfaces to handle the extraction.

	* RowMapper	(map from one tuple to one domain object)
	* ResultExtractor   (A general extractor to convert tuples to one domain object. SingleValueResultExtractor is a special implementation of ResultExtractor. Use it when your pig script has only one value in output, e.g. total number of unique visitors of your web site)


### Provide utility class for construct Tuple and DataBag ###
The traditional way to build a Tuple is to first create TupleFactory, and create tuple using TupleFactory then add element to this tuple. This way is straightforward but not so concise.
RFA-PIG provide two utility class for constructing Tuple and DataBag. The class name follows the factory name convention, just like Arrays and Collections in JDK.
The method of these two classes is simple and easy to understand ,such has Tuples.newTuple(E... elements) which let you build Tuple from arbitrary number of elements. You can refer javadoc for more details.


### Provide utility class for formating Tuple and DataBag ###

Users sometimes complain that the Pig's
internal plain text representation of Tuple and DataBag, especially there's nested Tuple and DataBag. It is hard to
parse it for users outside pig world, especially when handling result using other
programming languages. 
e.g. You have a result as following <br/>
<code><pre>
  (John,(1,2,3))
  (Lucy,(2,3,4)}
</pre></code>
You can use this class ResultFormat to convert it into the following format which you can handle it easily. <br/>
<code><pre>
  John,1,2,3
  Lucy,2,3,4
</pre></code>
You can refer javadoc for more details.

## Commit Back! ##

Bug fixes, features are welcome especially documentation improvement, because I am not a native English speaker, some documents may not so clear and concise!  Please fork and send me a pull request on github, 
and I will do my best to keep up.  If you make major changes, add yourself to the contributors list below.

## Contributors ##

* zjffdu [(zjffdu@gmail.com) (http://zjffdu.blogspot.com)]
