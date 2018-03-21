import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._

import java.util.Date
import org.apache.log4j.{Level, Logger}

/**
 *
 * Use this as starting point for performing sentiment analysis on tweets from Twitter*
 *  To run this app
 *   ./sbt/sbt assembly
 *   $SPARK_HOME/bin/spark-submit --class "TwitterStreamingApp" --master local[*] ./target/scala-2.10/twitter-streaming-assembly-1.0.jar
 *      <consumer key> <consumer secret> <access token> <access token secret>
 */
object TwitterStreamingApp {
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: TwitterStreamingApp <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //val sparkConf = new SparkConf().setAppName("arvadive")
    // to run this application inside an IDE, comment out previous line and uncomment line below
    val sparkConf = new SparkConf().setAppName("arvadive").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val stopWordsRDD = ssc.sparkContext.textFile("src/main/resources/stop-words.txt")
    val posWordsRDD = ssc.sparkContext.textFile("src/main/resources/pos-words.txt")
    val negWordsRDD = ssc.sparkContext.textFile("src/main/resources/neg-words.txt")

    val positiveWords = posWordsRDD.collect().toSet
    val negativeWords = negWordsRDD.collect().toSet
    val stopWords = stopWordsRDD.collect().toSet

    val englishTweets = stream.filter(status => status.getUser().getLang() == "en")
    
       
    // Implementation:
    
       ssc.checkpoint("_checkpoint")
    //stream.checkpoint(Seconds(60))
       
    // remove the stop words and (. | , | !)
    val tweets = englishTweets.map(status => status.getText().toLowerCase.dropWhile((_ == ',')).dropWhile(_ == '.').dropWhile(_ == '!'))
    val tweetwords = tweets.map(tweet0 => (tweet0, tweet0.split(" ")))
    val withoutStopWords = tweetwords.mapValues(tweet1 => tweet1.filter(!stopWords.contains(_)))
    
     // score the tweets
    val TweetScore= withoutStopWords.mapValues{tweet2 => (tweet2.map(e => if (positiveWords.contains(e)) 1 else if (negativeWords.contains(e)) -1 else 0)).map(_.toInt).sum}
    val TweetScoreRated= TweetScore.map{case(tweet, score) => (tweet, (if (score >0) "POSITIVE"  else if (score <0) "NEGATIVE" else "NEUTRAL"))}
    
    //Debug: print the tweet and its score
    TweetScoreRated.print()
    
    // Reduce by Window size and print
    
    val TweetScoreCount10=TweetScoreRated.map{case(tweet, score) => (score,1)}.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), (a:Int,b:Int) => (a - b),Seconds(10), Seconds(2))
    TweetScoreCount10.foreachRDD { rdd =>  rdd.foreach { case (id, values) =>  println("(for Window 10s)count for " + id + " is: " + values) }}
    
    val TweetScoreCount30=TweetScoreRated.map{case(tweet, score) => (score,1)}.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), (a:Int,b:Int) => (a - b),Seconds(30), Seconds(2))
    TweetScoreCount30.foreachRDD { rdd =>  rdd.foreach { case (id, values) =>  println("(for Window 30s)count for " + id + " is: " + values) }}
    
    ssc.start()
    ssc.awaitTermination()
  }

}
