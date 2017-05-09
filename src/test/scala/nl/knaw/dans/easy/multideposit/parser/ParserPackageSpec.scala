package nl.knaw.dans.easy.multideposit.parser

import nl.knaw.dans.easy.multideposit.UnitSpec

class ParserPackageSpec extends UnitSpec {

  "find" should "return the value corresponding to the key in the row" in {
    val row = Map("foo" -> "abc", "bar" -> "def")
    row.find("foo").value shouldBe "abc"
  }

  it should "return None if the key is not present in the row" in {
    val row = Map("foo" -> "abc", "bar" -> "def")
    row.find("baz") shouldBe empty
  }

  it should "return None if the value corresponding to the key in the row is empty" in {
    val row = Map("foo" -> "abc", "bar" -> "")
    row.find("bar") shouldBe empty
  }

  it should "return None if the value corresponding to the key in the row is blank" in {
    val row = Map("foo" -> "abc", "bar" -> "  \t  ")
    row.find("bar") shouldBe empty
  }
}
