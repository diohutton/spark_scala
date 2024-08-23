import Cases.{Flight, FlightBuddies, FlightBuddiesWithDates, FlightWithDate, FlightsByMonth, FrequentFlyer, Passenger, PassengerFlight, PassengerRuns}
import Utilities.{concatCountries, findLongestRun, removeDuplicateCountries}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, Month}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import scala.collection.mutable.{ListBuffer, Set}

object Main {

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
    //limit sorted_flights to distinct Flight Id
    val uniqueFlights = sorted_flights.dropDuplicates("flightId").as[Flight]
    val flightsPerMonthDF = findFlightsByMonth(uniqueFlights).toDF(monthFlightsColumns:_*)
    flightsPerMonthDF.write.mode("overwrite").option("header", "true").csv("output/flights-per-month")

    // Question 2: Find names of most frequent flyers
    val frequentFlyerColumns = Seq("Passenger ID","Number of Flights","First Name","Last Name")
    val frequentFlyersDF = findTopFrequentFlyers(passengerFlights).toDF(frequentFlyerColumns:_*)
    frequentFlyersDF.write.mode("overwrite").option("header", "true").csv("output/top-flyers")

    // Question 3: Find greatest number of countries a passenger has been in without being in the UK
    val longestRunColumns = Seq("Passenger ID", "Longest Run")
    val longestRunDF = findLongestCountryRunByPassenger(passengerFlights).toDF(longestRunColumns:_*)
    longestRunDF.write.mode("overwrite").option("header", "true").csv("output/longest-runs")

    //Question 4: Find passengers that have been on more than 3 flights together
    val flightsTogetherColumns = Seq("Passenger 1 ID", "Passenger 2 ID", "Number of Flights Together")
    val flightsTogetherDF =  findFrequentFlightBuddies(passengerFlights).toDF(flightsTogetherColumns:_*)
    flightsTogetherDF.write.mode("overwrite").option("header", "true").csv("output/flights-together")

    //Question 5: Find passengers that have been on N flights together in the designated date range
    //example 1:
    val flightsTogetherDatesColumns = Seq("Passenger 1 ID", "Passenger 2 ID", "Number of Flights Together", "From", "To")
    val flightsTogetherDatesDF1 = findFrequentFlightBuddiesByDateAndCount(passengerFlights, "2017-01-01", "2017-04-30", 6).toDF(flightsTogetherDatesColumns:_*)
    flightsTogetherDatesDF1.write.mode("overwrite").option("header", "true").csv("output/flights-together-date-1")

    //example 2:
    val flightsTogetherDatesDF2 = findFrequentFlightBuddiesByDateAndCount(passengerFlights, "2017-08-01", "2017-12-31", 8).toDF(flightsTogetherDatesColumns: _*)
    flightsTogetherDatesDF2.write.mode("overwrite").option("header", "true").csv("output/flights-together-date-2")
  }

  /**
   * For each month in the dataset, accumulate the number of flights and
   * print report to file
   * @param dataset full set of flight data
   */
  private def findFlightsByMonth(dataset: Dataset[Flight]): Seq[FlightsByMonth] = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
    val monthMap = scala.collection.mutable.Map[Int, FlightsByMonth]()
    dataset.collect.foreach(p => {
      val monthInt = Month.from(LocalDate.parse(p.date, formatter)).getValue
      if (monthMap.contains(monthInt)) {
        val flightMonth = monthMap.apply(monthInt)
        flightMonth.numFlights += 1
        monthMap(monthInt) = flightMonth
      } else {
        monthMap.put(monthInt, FlightsByMonth(monthInt, 1))
      }
    })

    monthMap.values.toList.sortBy(_.month)
  }

  /**
   * Counts number of flights per passenger and prints results to file in descending order
   * @param dataset full set of passenger flight data
   */
  private def findTopFrequentFlyers(dataset: Dataset[PassengerFlight]): Seq[FrequentFlyer] = {
    //aggregate number of flights per passenger
    val frequentFlyersMap = scala.collection.mutable.Map[Int, FrequentFlyer]()
    dataset.collect().foreach(p => {
      if (frequentFlyersMap.contains(p.passengerId)) {
        val tempFlyer = frequentFlyersMap.apply(p.passengerId)
        tempFlyer.numFlights += 1
        frequentFlyersMap(p.passengerId) = tempFlyer
      } else {
        frequentFlyersMap.put(p.passengerId, FrequentFlyer(p.passengerId, 1, p.firstName, p.lastName))
      }
    })
    //sort by number of flights in descending order
    frequentFlyersMap.values.toList.sortBy(_.numFlights).reverse.take(100)
  }

  /**
   * For each passenger in the dataset, accumulate from/to countries into a sequence and
   * determine the longest run of countries visited without being in the UK. Sort descending
   * by longest run
   * @param dataset full set of passenger flight data
   */
  private def findLongestCountryRunByPassenger(dataset: Dataset[PassengerFlight]): Seq[PassengerRuns] = {
    //aggregate countries visited by each passenger
    val countriesMap = scala.collection.mutable.Map[Int, String]()
    dataset.collect.foreach(p => {
      if (countriesMap.contains(p.passengerId)) {
        var tempVal = countriesMap.apply(p.passengerId)
        tempVal += concatCountries(p.from, p.to)
        countriesMap(p.passengerId) = tempVal
      } else {
        countriesMap.put(p.passengerId, concatCountries(p.from, p.to))
      }
    })

    //process each passenger's countries sequence and find longest run
    val passengerCountriesList = ListBuffer[PassengerRuns]()
    for ((pId, countries) <- countriesMap) {
      val changed = removeDuplicateCountries(countries.split(",").toList)
      val number = findLongestRun(changed)
      passengerCountriesList += PassengerRuns(pId, number)
    }
    passengerCountriesList.sortBy(_.longestRun).reverse
  }


  /**
   * Map all flights a passenger has taken, if they have been on at least 3 flights, compare to
   * other passengers' flights to determine how many flights they have been on together
   * @param dataset full set of passenger flight data
   */
  private def findFrequentFlightBuddies(dataset: Dataset[PassengerFlight]): Seq[FlightBuddies] = {
    //create initial map of passengerId with a Set of flights taken
    val flightSetsMap = scala.collection.mutable.Map[Int, Set[Int]]()
    dataset.collect.foreach(p => {
      if (flightSetsMap.contains(p.passengerId)) {
        var tempSet = flightSetsMap.apply(p.passengerId)
        tempSet += p.flightId
        flightSetsMap(p.passengerId) = tempSet
      } else {
        flightSetsMap.put(p.passengerId, Set(p.flightId))
      }
    })

    //remove map entries if flightSet not at least 3
    val filteredFlightsMap = flightSetsMap.filter((f) => (f._2.size > 3))
    val resultMap = scala.collection.mutable.Map[(Int,Int), Int]()

    //compare passengers flight sets with each other, count their intersection and
    //add to final result if flight size is greater than 3
    filteredFlightsMap.keys.foreach((firstP) => {
      val firstSet = filteredFlightsMap.apply(firstP)
      for ((secondP, secondSet) <- filteredFlightsMap) {
        //do not combine keys that match, do not add matching tuple keys ie, (3, 6) and (6, 3)
        if (firstP != secondP) {
           if (!resultMap.contains((firstP, secondP)) && !resultMap.contains((secondP, firstP))) {
             val intersect = firstSet.intersect(secondSet)
             if (intersect.size >= 3) {
               resultMap.put((firstP, secondP), intersect.size)
             }
           }
        }
      }
    })
    val flightBuddiesList = ListBuffer[FlightBuddies]()
    for ((key, count) <- resultMap) {
      flightBuddiesList += FlightBuddies(key._1, key._2, count)
    }
    flightBuddiesList.sortBy(_.flightsTogether).reverse
  }

  /**
   * * Map all flights a passenger has taken, if they have been on at least numFlights, compare to
   * other passengers' flights to determine if they have been on numFlights together in the designated
   * start-end timeframe
   *
   * @param dataset full set of passenger flight data
   * @param start beginning date of search
   * @param end ending date of search
   * @param numFlights flights taken together
   */
  private def findFrequentFlightBuddiesByDateAndCount(dataset: Dataset[PassengerFlight], start: String, end: String, numFlights: Int): Seq[FlightBuddiesWithDates] = {
    //restrict date range to between start and end, create initial map of passengerId with a Set of flights taken
    val betweenDatesDS = dataset.filter(col("date").between(start, end))
    val flightDateMap = scala.collection.mutable.Map[Int, FlightWithDate]()
    betweenDatesDS.collect.foreach(p => {
      if (flightDateMap.contains(p.passengerId)) {
        val tempFWD = flightDateMap.apply(p.passengerId)
        tempFWD.flights += p.flightId
        tempFWD.dates += p.date
        flightDateMap(p.passengerId) = tempFWD
      } else {
        flightDateMap.put(p.passengerId, FlightWithDate(Set(p.flightId), scala.collection.mutable.Set(p.date)))
      }
    })

    //remove map entries if flightSet not at least numFlights
    val filteredFlightsMap = flightDateMap.filter((f) => (f._2.flights.size >= numFlights))
    val resultMap = scala.collection.mutable.Map[(Int,Int), FlightBuddiesWithDates]()
      //compare passengers flight sets with each other, count their intersection and
      //add to final result if flight size equals numFlights
      filteredFlightsMap.keys.foreach((firstP) => {
        val firstSet = filteredFlightsMap.apply(firstP)
        for ((secondP, secondSet) <- filteredFlightsMap) {
          //do not combine keys that match, do not add matching tuple keys ie, (3, 6) and (6, 3)
          if (firstP != secondP) {
            if (!resultMap.contains((firstP, secondP)) && !resultMap.contains((secondP, firstP))) {
              val intersect = firstSet.flights.intersect(secondSet.flights)
              if (intersect.size >= numFlights) {
                val intersectDates = firstSet.dates.intersect(secondSet.dates).toSeq.sorted
                resultMap.put((firstP, secondP), FlightBuddiesWithDates(firstP, secondP, intersect.size, intersectDates.head, intersectDates.last))
              }
            }
          }
        }
      })
     resultMap.values.toList.sortBy(_.flightsTogether).reverse
    }
}
