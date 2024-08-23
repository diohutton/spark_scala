import Cases._
import Utilities.findLongestStringRun
import org.apache.spark.sql.functions.{array, asc, col, collect_list, concat_ws, count, desc, first, flatten, max, min, month, struct}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer


object MainWithSparkSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("qtexan").getOrCreate();
    import spark.implicits._

    // Merge csv files into a typed master DataSet[PassengerFlight]
    val sorted_passengers = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv("src/main/resources/passengers.csv").sort("passengerId").as[Passenger]

    val sorted_flights = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv("src/main/resources/flightData.csv").sort("passengerId", "date").as[Flight]

    val passengerFlights = {
      sorted_flights.join(sorted_passengers, Seq("passengerId")).as[PassengerFlight]
    }

    // Question 1: Find total number of flights per month
    val monthFlightsColumns = Seq("Month", "Number of Flights")
    val monthCount = sorted_flights.dropDuplicates("flightId").withColumn("month", month(col("date"))).
        groupBy(col("month")).count().orderBy(asc("month")).toDF(monthFlightsColumns: _*)
    monthCount.coalesce(1).write.mode("overwrite").option("header", "true").csv("spark-output/flights-per-month")

    // Question 2: Find names of most frequent flyers
    val frequentFlyerColumns = Seq("Passenger ID","Number of Flights","First Name","Last Name")
    val topFlyers = passengerFlights.groupBy("passengerId").
        agg(count("passengerId").alias("Number of Flights"),
        first("firstName").alias("First Name"),
        first("lastName").alias("Last Name")).
        orderBy(desc("Number of Flights")).toDF(frequentFlyerColumns:_*)
    topFlyers.coalesce(1).write.mode("overwrite").option("header", "true").csv("spark-output/top-flyers")

    // Question 3: Find greatest number of countries a passenger has been in without being in the UK
    val countryRuns = passengerFlights.groupBy("passengerId").
        agg(flatten(collect_list(array($"from", $"to"))).as("countries")).as[PassengerCountries]
    val countriesToString = countryRuns.withColumn("countries",
      concat_ws(",", col("countries"))).as[PassengerCountries]

    val passengerCountriesList = ListBuffer[PassengerRuns]()
    countriesToString.collect.foreach(p => {
      val numRuns = findLongestStringRun(p.countries)
      passengerCountriesList += PassengerRuns(p.passengerId, numRuns)
    })

    val pcDS = passengerCountriesList.toDS().orderBy(desc("longestRun"))
    pcDS.coalesce(1).write.mode("overwrite").option("header", "true").csv("spark-output/longest-runs")


    //Question 4: Find passengers that have been on more than 3 flights together
    val flightsTogetherColumns = Seq("Passenger 1 ID", "Passenger 2 ID", "Number of Flights Together")
    val flightBuddies = passengerFlights.as("df1").join(passengerFlights.as("df2"),
        col("df1.passengerId") < col("df2.passengerId") &&
          col("df1.flightId") === col("df2.flightId"), "inner").
        groupBy("df1.passengerId", "df2.passengerId").
        agg(count("*").as("flightsTogether")).where(col("flightsTogether") >= 3).orderBy(desc("flightsTogether"))
        .toDF(flightsTogetherColumns:_*)
    flightBuddies.coalesce(1).write.mode("overwrite").option("header", "true").csv("spark-output/flights-together")

    //Question 5: Find passengers that have been on N flights together in the designated date range
    //example 1:
    findFrequentFlightBuddiesByDateAndCount(passengerFlights, "2017-01-01", "2017-04-30", 6, "flights-together-date-1")
    //example 2:
    findFrequentFlightBuddiesByDateAndCount(passengerFlights, "2017-08-01", "2017-12-31", 8, "flights-together-date-2")
  }

  private def findFrequentFlightBuddiesByDateAndCount(dataset: Dataset[PassengerFlight], start: String, end: String, numFlights: Int, dirName: String): Unit = {
    //restrict date range to between start and end, create initial map of passengerId with a Set of flights taken
    val betweenDatesDS = dataset.filter(col("date").between(start, end))
    val flightsTogetherDatesColumns = Seq("Passenger 1 ID", "Passenger 2 ID", "Number of Flights Together", "From", "To")
    val flightBuddies = betweenDatesDS.as("df1").join(betweenDatesDS.as("df2"),
      col("df1.passengerId") < col("df2.passengerId") &&
        col("df1.flightId") === col("df2.flightId"), "inner").
      groupBy("df1.passengerId", "df2.passengerId").
      agg(count("*").as("flightsTogether"), min(col("df1.date")).as("from"),
        max(col("df1.date")).as("to")).
      where(col("flightsTogether") >= numFlights).
      orderBy(desc("flightsTogether"))
      .toDF(flightsTogetherDatesColumns: _*)

    flightBuddies.coalesce(1).write.mode("overwrite").option("header", "true").csv("spark-output/"+ dirName)
  }
}
