
object Utilities extends App {
  /**
   * Given a sequence of countries, determine the longest run of countries a
   * passenger has been in without being in the UK
   * @param countries
   * @return
   */
  def findLongestRun(countries: Seq[String]): Int = {
    var numRun: Int = 0
    def longestRun(countries: Seq[String], idx: Int): Int = {
      if (idx < countries.length) {
        val tempIdx = countries.segmentLength(_ !="uk", idx)
        if (tempIdx > numRun) {
          numRun = tempIdx
        }
        longestRun(countries, idx + 1)
      } else {
        numRun
      }
    }
    longestRun(countries, 0)
  }

  /**
   * Given a string of countries, split by delimeter uk,
   * remove duplicate countries and return the longest length
   * @param countries
   * @return
   */
  def findLongestStringRun(countries: String): Int = {
    var numRun: Int = 0
    val parts = countries.split("uk")
    parts.foreach(p => {
      val nodupes = p.split(",").toList.distinct.filter(_.nonEmpty)
      if (nodupes.size > numRun){
        numRun = nodupes.size
      }
    })
    numRun
  }
  /**
   * Given all the from, to destinations for a passenger's flights, remove
   * duplicate countries in the sequence unless uk is found
   * @param countries full sequence of from and to destination countries
   * @return sequence with duplicate countries other than UK removed
   */
  def removeDuplicateCountries(countries: Seq[String]): Seq[String] = {
      countries.foldLeft(List.empty[String]) { (partialResult, element) =>
        if (partialResult.contains(element) && element != "uk") partialResult
        else partialResult :+ element
      }
  }

  /**
   * Utility method to add commas between from and to countries
   * @param from
   * @param to
   * @return
   */
  def concatCountries(from: String, to: String): String = {
      from + "," + to + ","
  }
}
