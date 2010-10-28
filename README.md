# RAF-Pig (Rich API for Pig) #

## To use
1. git clone
2. maven package
3. check out target/dw.pig-version.jar


## The sugars RAF-Pig provides

### Asynchronously execute pig script

The original way to run pig script in embeded mode is create a PigServer, then call PigServer.register() to run pig script(this api is blocking mode). 
If your script has dump or store statement, you have to wait for a long time. Or you can call PigServer.openIterator() which is also blocking mode.
Actually pig client has not so much thing to been involved in the execution of pig script. You can do other thing on client when you run Pig script on hadoop cluster. 
To improve the efficiency, RAF-Pig provides a Asynchronous way to run pig script. The following is an simple example:
<code><pre>
 		PigConfiguration conf = new PigConfiguration();
        PigSession session = new PigSession(conf);
        PigJob job = PigJobs.newPigJob(new File("scripts/Test.pig")).setJobName("test-job").setPigJobListener({
          	@Override
	        public void beforeStart(PigJob job) throws Exception {
	            System.out.println("Start");
	        }
	
	        @Override
	        public void onSucess(PigJob job) throws Exception {
	            Iterator<Page> iter = job.getOutput("raw",
	                    "PigStorage()", "data/output");
	            while (iter.hasNext()) {
	                System.out.println(iter.next());
	            }
	        }
        	
        )};
        
		session.submitPigJob(job);  // this method is non-blocking
</pre></code>

Here, we introduce three important classes for RAF-Pig. PigConfiguration encapsulate common property for your PigJobs, and You can consider PigSession as a session between you and Hadoop cluster, 
and you can submit PigJob through this class. PigJob is the basic interface which wrap a concrete pig script and any payload around PigJob. You can setJobName,setPriority,setParameter and setScriptSource through class PigJob. 
One thing to be noted here is that the script source is InputStream that means .


### Add hook before and after pig script execution.

Since we provide a Asynchronous way to run pig script,t then how do you get the result. PigJob has a method setPigJobListener which you allow you hook method before and after the lifecyle of pig script execution.
interface PigJobListener has three methods beforeStart,onSucess,onFailure. It is very easy to guess the intention of these methods. And we provide two implemetation for users. SimplePigJobListener which just print log message in the three stages. and PibJobListernAdapter which provider a adapter class.


### Provide extract pattern for convert pig data structure to your domain data structure.

What we get from pig script's result is tuples which is less semantics for specially application. You may want to convert pig data structure to your domain data structure. RAF-Pig provide three extract interface to handle the extraction
#### <li>  ResultExtractor   (A general extractor to convert tuples to one domain object)
#### <li>  RowMapper		 (map from one tuple to one domain object)
#### <li>  SingleValueResultExtractor	 
 Use it when your pig script has only one value in output, e.g. total number of visitors of your web site.
  <pre>
  a = load 'input_location' as (uuid,....);
  b = foreach a generate uuid;
  c = group b all;
  d = foreach c generate COUNT(b.uuid);
  store d into 'output_location';
  </pre>
#### <li>  MapResultExtractor
  Use it when you'd like to convert pig result to map type, e.g. 
  <pre>
  a = load 'input_location' as (name:chararray,count:int);
  b = group a by name;
  c = foreach b generate group,SUM(a.count);
  store c into 'output_location';
  </pre>
  
### Provide utility class for construct Tuple and DataBag.
The traditional way to build a Tuple is first create TupleFactory, and create tuple using TupleFactory then add element to this tuple. This way is straightforward but not so consise.
RFA-Pig provide two utility class for constructing Tuple and DataBag. The class name follows the name converions , just like Arrays and Collections
the method of these two classes is simple and easy to understand ,such has Tuples.newTuple(E... elements) which let you build Tuple from arbitrary number of elements.
<li>Tuples
<li>Bags

### Provider utility class for format Tuple and DataBag


### Commit Back! ###

Bug fixes, features, and documentation improvements are welcome!  Please fork and send me a pull request on github, and I will do my best to keep up.  If you make major changes, add yourself to the contributors list below.

### Contributors ###

* zjffdu [(zjffdu@gmail.com) (http://zjffdu.blogspot.com)]
