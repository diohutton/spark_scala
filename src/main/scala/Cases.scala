import scala.collection.mutable.{Set}

object Cases {

  // class that maps to flightData.csv
  case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)

  // Output class for passengers who have been on same flights with count
  case class FlightBuddies(firstPassenger: Int, secondPassenger: Int, flightsTogether: Int)

  // Output class for passengers who have been on same flights with count in a date range
  case class FlightBuddiesWithDates(firstPassenger: Int, secondPassenger: Int, flightsTogether: Int, from: String, to: String)

  // class for accumulating number of flights per month
  case class FlightsByMonth(month: Int, var numFlights: Int)

  // Store passenger flight numbers and flight dates in two sets
  case class FlightWithDate(val flights: Set[Int], val dates: Set[String])

  // class for accumulating number of flights for a passenger
  case class FrequentFlyer(passengerId: Int, var numFlights: Int, firstName: String, lastName: String)

  // class that maps to passengers.csv
  case class Passenger(passengerId: Int, firstName: String, lastName: String)

  // class to collect a passengers countries
  case class PassengerCountries(passengerId: Int, countries: String)

  // schema of full passenger flight data
  case class PassengerFlight(passengerId: Int, flightId: Int, from: String, to: String, date: String,
                             firstName: String, lastName: String)

  // passenger id with longest run of countries visited
  case class PassengerRuns(passengerId: Int, longestRun: Int)
}
