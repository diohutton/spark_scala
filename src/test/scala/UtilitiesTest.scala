import org.scalatest.funsuite.AnyFunSuite

class UtilitiesTest extends AnyFunSuite {
  test("Utilities.concatCountries") {
    val resultString = "ca,zb,"
    val resultString2 = "ab,gh,"
    assert(Utilities.concatCountries("ca", "zb") === resultString)
    assert(Utilities.concatCountries("ab", "gh") === resultString2)
  }

  test("Utilities.findLongestRun") {
    val testString =  Seq("uk", "fr", "us", "cn", "uk", "de", "tl")
    assert(Utilities.findLongestRun(testString) === 3)

    val testString2 =  Seq("id", "fr", "uk", "cn", "it", "de", "tl", "uk", "sp")
    assert(Utilities.findLongestRun(testString2) === 4)
  }

  test("Utilities.findLongestStringRun") {
    val testString:String = "uk, fr, mx, us, cn, uk, de, tl, fr, mx, mx, it"
    val testString2:String = "uk, fr, us, mx, us, mx, cn, uk, de, tl, fr, mx, it, uk, us, de, de, sw, sw, ca, pg, cz"

    assert(Utilities.findLongestStringRun(testString) === 5)
    assert(Utilities.findLongestStringRun(testString2) === 6)
  }

  test("Utilities.removeDuplicateCountries"){

    val testString = Seq("uk", "fr", "mx", "us", "cn", "uk", "de", "tl", "fr", "mx")
    val resultString = Seq("uk", "fr", "mx", "us", "cn", "uk", "de", "tl")
    assert(Utilities.removeDuplicateCountries(testString) === resultString)

    val testString2 = Seq("id", "fr", "uk", "id", "cn", "it", "de", "tl", "uk", "sp", "fr", "cn", "uk")
    val resultString2 = Seq("id", "fr", "uk", "cn", "it", "de", "tl", "uk", "sp", "uk")
    assert(Utilities.removeDuplicateCountries(testString2) === resultString2)
  }
}
