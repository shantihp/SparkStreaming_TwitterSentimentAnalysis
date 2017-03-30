package ucsc.spark

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.PropertyConfigurator


object TwitterStreamingApp {
    
    def main(args: Array[String]) {
        val baseDir = System.getProperty("user.dir")
        PropertyConfigurator.configure(baseDir + "/resources/log4j.properties");
        
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

        //val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
        // to run this application inside an IDE, comment out previous line and uncomment line below
        val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(2))
        ssc.checkpoint(baseDir + "/checkpoint")
        val stream = TwitterUtils.createStream(ssc, None, filters);

        val stopWords = ssc.sparkContext.textFile("resources/stop-words.txt").collect().toSet
        val positiveWords = ssc.sparkContext.textFile("resources/pos-words.txt").collect().toSet
        val negativeWords = ssc.sparkContext.textFile("resources/neg-words.txt").collect().toSet

        val englishTweets = stream.filter(status => status.getLang() == "en")
        val tweets = englishTweets.map(status => status.getText())
        //tweets.print()
    
        // create a pair of (tweet, array of words) for each tweet
        val tweetAndWords = tweets
            .map(x => (x, x.split(" ")))
            .mapValues(filterWords(_, stopWords))

        // create a pair of (tweet, sentiment) for each tweet
        val tweetAndSentiment = tweetAndWords.mapValues(getSentiment(_, positiveWords, negativeWords))

        // Maintain # of positive, negative and neutral sentiment counts for 10 seconds window
        val sentimentCountsWindow10 = tweetAndSentiment
            .map(x => (x._2, 1))
            .reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(10), Seconds(10))
            
        // Maintain # of positive, negative and neutral sentiment counts for 30 seconds window
        val sentimentCountsWindow30 = tweetAndSentiment
            .map(x => (x._2, 1))
            .reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(30))

        // print results to console
        tweetAndSentiment.print()
        sentimentCountsWindow10.print()
        sentimentCountsWindow30.print()

        ssc.start()
        ssc.awaitTermination()
    }

    def filterWords(words: Array[String], stopWords: Set[String]): Array[String] = {
        val punctuationMarks = "[.,!?\"\\n]".r
        val firstLastNonChars = "^\\W|\\W$".r
        val filteredWords = words
            .map(_.toLowerCase())
            .map(punctuationMarks.replaceAllIn(_, ""))
            .map(firstLastNonChars.replaceAllIn(_, ""))
            .filter(!_.isEmpty())
            .filter(!stopWords.contains(_))
        
        return filteredWords
    }

    def getSentiment(wordsArray: Array[String], positiveWords: Set[String], negativeWords: Set[String]): String = {
        var numPositive = 0;
        var numNegative = 0;
        var numNeutral = 0;
        
        for (words <- wordsArray){
            if (positiveWords.contains(words))
                numPositive += 1
            else if (negativeWords.contains(words))
                numNegative += 1
            else
                numNeutral +=1
        }
        

        if ((numPositive - numNegative) > 0) 
            return "Positive"
        else if ((numPositive - numNegative) < 0)
            return "Negative"
        else
            return "Neutral"
    }
}
