import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.logging.log4j.LogManager
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataOutputStream

object WebLogsChallenge  {

  var maxIdlePeriod = 60      //  maximum idle period between 2 requests in a session
  var sessionId: Int = 0;

  // UDF to remove port and extract IP
  def cleanIP(ip: String): String = {
    return ip.split(":")(0);
  }

  // UDF to extract url
  def cleanURL(url: String): String = {
    return url.split(" ")(1);
  }

  // FUNCTION FOR ASSIGNING session ID
  // A static variable 'sessionId' is created and initialized to 0.
  //  Max Idle period between 2 requests is defined using a param 'maxIdlePeriod'.
  // (maxIdlePeriod is accepted as command line arg. By default it is 1 min if not specified.
  // If it is a new entry or if the timestamp difference between the current and previous entries is more than maxIdlePeriod, then sessionId is incremented
  // Else the previous session id is used
  def fetchSessionId(timeDiff: Int): Int = {
    // Check whether its a new IP-client combination or an existing combination appearing after a delayed time
    // and increment the session id correspondingly
    if (timeDiff == 0 || timeDiff > maxIdlePeriod)

      sessionId += 1
    return sessionId;

  }

  val fetchSessionIdUDF = udf(fetchSessionId _)
  val cleanIPUDF = udf(cleanIP _)
  val cleanURLUDF = udf(cleanURL _)


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Web Logs Challenge").setMaster(args(0))
//    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.master(args(0)).appName("Spark CSV").getOrCreate
    spark.sparkContext.setLogLevel("WARN")
    var targetPath = "data/output"
    if (args.length > 2)
      targetPath = args(2)

    val fs = FileSystem.get(new Configuration())
    val outputPath = new Path(targetPath)
    if (fs.exists(outputPath))
      fs.delete(outputPath,true)

    if (args.length > 3)
      maxIdlePeriod = 60 * Integer.parseInt(args(3))  // MaxIdlePeriod : default 1 minute. Can be configured through command line

    // Read data into dataframe
    val df = spark.read.format("csv").
      option("quote", "\"").
      option("delimiter", " ").
      option("inferSchema", "true").
      csv(args(1))
    println("Num. of records = " + df.count())

    // Select timestamp, IP, URL and client
    var fieldNames = Seq("ts", "ip", "url", "client")
    var fields = df.select("_c0", "_c2", "_c11", "_c12").toDF(fieldNames: _*).orderBy()

    // Clean IP and URL
    fields = fields.withColumn("ip", cleanIPUDF(col("ip")))
    fields = fields.withColumn("url", cleanURLUDF(col("url")))
    fields.show()


    // Define a window corresponding to a particular IP, client combination.
    // Within each window we use maxIdlePeriod to determine sessions. Hence sort by timestamp
    var window = Window.partitionBy("ip", "client").orderBy("ts")

    // Append previous timestamp using lag function
    var fieldsEnr = fields.withColumn(colName = "prev_ts", lag("ts", 1).over(window))

    //////////////////////////////////////////////////////////////////////////////////////////////
    // I. SESSIONIZATION
    // -----------------
    // The assumption here is that, the requests from the
    // same IP(without port) and same client that occur within maxIdlePeriod
    // constitute a session. maxIdlePeriod is configurable, by default it is 1 minute (60 seconds)
    //////////////////////////////////////////////////////////////////////////////////////////////

    sessionId=0
    // create column timediff that stores the difference between the timestamps of the current and previous row
    fieldsEnr = fieldsEnr.withColumn(colName = "timediff", unix_timestamp(col("ts")) - unix_timestamp(col("prev_ts")))
    fieldsEnr = fieldsEnr.withColumn(colName = "timediff", when(col("timediff").isNull,0).otherwise(col("timediff")))
    fieldsEnr.show()


    // determine session by comparing timestamp, ip and client with previous row values using fetchSessionId UDF
    fieldsEnr.withColumn(colName = "session_id", fetchSessionIdUDF(fieldsEnr("timediff"))).coalesce(1).write.csv(targetPath+"/sessionize_stg")


    // save as staging file and reload to avoid impact of static variable on sessionId
    fieldNames=Seq("ts","ip","url","client","prev_ts","timediff","session_id")
    var session=spark.read.format("csv").
      option("quote", "\"").
      option("delimiter", ",").
      option("inferSchema", "true").
      csv(targetPath+"/sessionize_stg").toDF(fieldNames: _*)

    //select required columns and write to final output
    println("1. Sessionization")
    var sessionizeOutput = session.select("session_id","ts","ip","client").coalesce(1)
    sessionizeOutput.write.option("header", "true").csv(targetPath+"/sessionize")
    sessionizeOutput.show()



    //////////////////////////////////////////////////////////////////
    // II. AVERAGE TIME SPENT
    // ---------------------
    // To calculate the average time spent, determine the max and min
    // timestamp for each session and ip calculate time interval between them
    // and calculate average across sessions
    //////////////////////////////////////////////////////////////////


    var intervals = sessionizeOutput.groupBy("session_id","ip").agg(max("ts"),min("ts"))
    intervals=intervals.withColumn("interval_seconds",unix_timestamp(col("max(ts)")) - unix_timestamp(col("min(ts)"))).sort("session_id")
    println("2. Average Time spent")
    intervals.coalesce(1).write.csv(targetPath+"/intervals")
    intervals.show()
    val avg=intervals.select(mean(intervals("interval_seconds")).alias("avg_time_spent_seconds"))
    avg.coalesce(1).write.option("header", "true").csv(targetPath+"/avg_session_length")
    println("Average session time: "+ avg.first().getDouble(0) +" seconds")


    //////////////////////////////////////////////////////////////////
    // III. UNIQUE URL HITS
    // ---------------------
    // To calculate unique url hits group by session id and count distinct urls
    //////////////////////////////////////////////////////////////////

    val urlHits=session.groupBy("session_id","ip","client").agg(countDistinct("url").alias("unique_hits"))
    urlHits.select("session_id","ip","unique_hits").coalesce(1).write.option("header", "true").csv(targetPath+"/url_hits")
    println("Unique URL hits: ")
    urlHits.show()

    //////////////////////////////////////////////////////////////////
    // IV. MOST ENGAGED USERS
    // ---------------------
    // To calculate most engaged users sort by interval column (from II) in descending order
    //////////////////////////////////////////////////////////////////

    var topUsers = intervals.sort(desc("interval_seconds"))
    topUsers.show(100)
    topUsers.select("session_id","ip","interval_seconds").coalesce(1).write.option("header", "true").csv(targetPath+"/top_users")


  }
}