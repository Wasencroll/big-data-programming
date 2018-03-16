/**
* University of Skövde | Big data programming | Assignment 2: Spark
* 
* HOW TO DEPLOY:
* http://spark.apache.org/docs/latest/quick-start.html#self-contained-applications
* C:/spark-2.0.2/bin/spark-submit --class Comments --master local[*] target/scala-2.11/reddit-comments_2.11-1.0.jar
*/

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/** SQL FUNCTIONS FOR DATA FRAMES e.g. avg("..."), orderBy(desc("...")).
* http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$ */
import org.apache.spark.sql.functions._


object Comments {
	def main(arg: Array[String]) = {
		/** SPARK CONFIGURATION
		* http://spark.apache.org/docs/latest/configuration.html
		*
		* The app name is shown in the Spark web UI: localhost:4040
		 */
		val conf = new SparkConf().setAppName("Reddit comments")


		/** Create Spark context in case you're working with RDDs. */
		val sc = new SparkContext(conf)
		// You can silence the console output a bit...
		sc.setLogLevel("WARN")


		/** Create Spark session for working with data frames. */
		val spark = SparkSession.builder().getOrCreate()
		// Needed import if you want to use $ for columns e.g. $"author"...
		import spark.implicits._



		// PATH TO DATA. Check if it is correct.
		val dataPath = "C:/Spark/reddit/2010"
		// PATH TO STOP WORDS FILE.
		val swPath = "res/stopwords.txt"


		// ======================== YOUR CODE =============================
		// Remember to use data frames.
		
		// loading data from source into dataframe
		val reddit_raw = spark.read.parquet(dataPath)

	
		// converting the UNIX Timestamp into a date format
		val redditDF = reddit_raw.select($"author", $"body", from_unixtime(col("created_utc").cast("int")) as "date", $"ups", $"name", $"parent_id", $"subreddit", $"subreddit_id")
		
		
		
		// ===== QUESTION 1 - comments per hour of the day =====
		// Grouping by Hour from Date Column and counting rows
		val countPerHour = redditDF.groupBy(hour(col("date")).as("Hour")).count
		
		// Saving to csv file
		countPerHour.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/commentcount")
		
		
		
		// ===== QUESTION 2 - Avg Ups per Subreddit =====
		// Grouping by Subreddit, Average of Ups per Subreddit and then sorting from High to Low
		val upsPerSubreddit = redditDF.groupBy(col("subreddit").as("Subreddit")).agg(avg(col("ups")).as("Ups Average")).sort(desc("Ups Average"))
		
		// Rounding Avg Value to 2 Decimal Places
		val roundedUpsPerSubreddit = upsPerSubreddit.select($"Subreddit", round($"Ups Average", 2))
		
		// Saving to csv file
		roundedUpsPerSubreddit.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/upsaverage")	
		
		
		
		// ===== QUESTION 3 - Comment with most ups per week of the year =====
		// Week, Ups, Comment
		val maxUps = redditDF.select(weekofyear($"date").as("Week"), $"ups", $"body")
		
		// Partitioning by Week and creating descending order by ups
		import org.apache.spark.sql.expressions.Window
		val w = Window.partitionBy($"Week").orderBy(desc("ups"))
		
		// filtering only the Top 1 comment in each Week group
		val bestComments = maxUps.withColumn("rank", rank.over(w)).where($"rank" <= 1)
		
		// Adapting output format
		val best52 = bestComments.select($"Week", $"ups" as "CommentUps", $"body" as "CommentBody").sort(asc("Week"))
		
		// Saving to Parquet file
		best52.coalesce(1).write.parquet("results/weekheights")
		
		
		
		// ===== QUESTION 4 =====
		// Taking the "body" column from every record of "redditDF" and splitting wherever whitespace exists, results in a new DF with one columns of all words and another column with count of these words, sorted descending
		val words = redditDF.select(explode(split(redditDF("body"), " ")).alias("word")).groupBy($"word").agg(count("*") as "count").sort(desc("count"))
		
		// Saving words to be filtered out to array
		val wrong = sc.textFile(swPath).collect
		
		// Filtering, by only taking words which are NOT in the "wrong" array, then limiting result to 200 records
		val top200words = words.filter(line => !wrong.contains(line(0).toString)).limit(200)
		
		// Saving Top 200 words to csv file
		top200words.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/wordcount")	


		// ================================================================

		

		sc.stop
	}
}