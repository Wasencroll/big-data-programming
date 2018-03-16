// ================ PRE-PROCESSING CODE =================
// Anything necessary to get the answers later.

// Loading Data into Spark
	val weather_raw = sc.textFile("C:/Users/alcroll/OneDrive - Deloitte (O365D)/Documents/HIS - Data Science/Big Data Programming/Spark/weather/2012")

// Selecting values of interest for assignment
	val weather = weather_raw.map(rdd => (	rdd.substring(15,23) + "," + rdd.substring(28,34) + "," + rdd.substring(34,41) + "," + rdd.substring(46,51) + "," + rdd.substring(65,69) + "," + 
												rdd.substring(69,70) + "," + rdd.substring(87,92) + "," + rdd.substring(92,93) + "," + rdd.substring(99,104) + "," + rdd.substring(104,105) ))
									
weather.take(5).foreach(println)

// Filtering longitude values for Sweden
	val max_lat = weather.filter(r => r.split(",")(1).toInt < +69047)
	val min_lat = max_lat.filter(r => r.split(",")(1).toInt > +55382)
	val max_lon = min_lat.filter(r => r.split(",")(2).toInt < +24034)
	val min_lon = max_lon.filter(r => r.split(",")(2).toInt > +11391)
	val qual1 = min_lon.filter(r => r.split(",")(5).toInt == 1)
	val qual2 = qual1.filter(r => r.split(",")(7).toInt == 1)
	val qual3 = qual2.filter(r => r.split(",")(9).toInt == 1)

// Filtering missing values
	val na_lat = qual1.filter(r => r.split(",")(1).toInt != +99999)
	val na_lon = na_lat.filter(r => r.split(",")(2).toInt != +999999)
	val na_elev = na_lon.filter(r => r.split(",")(3).toInt != +9999)
	val na_wind = na_elev.filter(r => r.split(",")(4).toInt != 9999)
	val na_temp = na_wind.filter(r => r.split(",")(6).toInt != +9999)
	val na_pres = na_temp.filter(r => r.split(",")(8).toInt != 99999)
	
// RDD for Sweden
	val weather_se = na_pres
	weather_se.persist


/** QUESTION 1
* How many measures (records) are there?
* 
* Answer: res4: Long = 1514332	
*/

// Your code/solution here...
// Counting records within RDD
	val count = weather_se.count


/** QUESTION 2
* Which is the highest elevation? Give latitude and longitude.
*
* Answer: 
* 
* - Elevetaion = +1146
* - Latitude = +67917
* - Longitude = +018600
*/

// Your code/solution here...
	val max_elevation = weather_se.reduce((a, b) => if ( a.split(",")(3) > b.split(",")(3) ) a else b)
	val elevation = max_elevation.split(",")(3)
	val latitude = max_elevation.split(",")(1)
	val longitude = max_elevation.split(",")(2)


/** QUESTION 3
* Which is the average wind speed in the country?
* 
* Answer: res10: Double = 3.775939292044281 m/s
*/

// Your code/solution here...
	val wind = weather_se.map(r => r.split(",")(4).toDouble)
	val avg_wind = wind.mean / 10


/** QUESTION 4
* Which is the average temperature per month? Sort from highest to lowest. 
* 
* Answer:
* - January = ???
* - February = ???
* - March = ???
* ...
*/

// Your code/solution here...
	val temp = weather_se.map{ r => val arr = r.split(",")
									val k = arr(0).substring(4,6).toInt
									val v = arr(6).toDouble / 10
									(k, v)
	}
	
	val map_temp = temp.mapValues(r => (r, 1))
	
	val reduce_temp = map_temp.reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2))
	
	val avg_temp = reduce_temp.map { x => 	val tmp = x._2
											val total = tmp._1
											val count = tmp._2
											(x._1, total / count)
	}

// ================ CONVERT TO DATAFRAME AND SAVE AS PARQUET =================
// convert to DATAFRAME
	import org.apache.spark._
	import org.apache.spark.sql._
	import org.apache.spark.sql.types._
	
	val sqlContext = new SQLContext(sc)
	val rowsWeather = weather_se.map{row => row.split(",")}.map{cols => Row(
																		cols(0), 
																		cols(1).toInt, 
																		cols(2).toInt,
																		cols(3).toInt,
																		cols(4).toInt,
																		cols(5).toInt,
																		cols(6).toInt,
																		cols(7).toInt,
																		cols(8).toInt,
																		cols(9).toInt)}
																
	val schema = StructType(List(
		StructField("date", StringType, false),
		StructField("latitude", IntegerType, false),
		StructField("longitude", IntegerType, false),
		StructField("elevation", IntegerType, false),
		StructField("windSpeed", IntegerType, false),
		StructField("windQuality", IntegerType, false),
		StructField("temperature", IntegerType, false),
		StructField("temperatureQuality", IntegerType, false),
		StructField("pressure", IntegerType, false),
		StructField("pressureQuality", IntegerType, false)))
																		
	val weatherDF = sqlContext.createDataFrame(rowsWeather, schema)
	
// saving as Parquet file
	weatherDF.write.parquet("C:/Users/alcroll/OneDrive - Deloitte (O365D)/Documents/HIS - Data Science/Big Data Programming/Spark/alexander-croll/part I/2010-parquet")
	
