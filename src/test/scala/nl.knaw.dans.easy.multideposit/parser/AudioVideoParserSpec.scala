/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit.parser

import better.files.File
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import nl.knaw.dans.easy.multideposit.model.{ AudioVideo, PlayMode, Springfield, SubtitlesFile }
import org.scalatest.BeforeAndAfterEach

trait AudioVideoTestObjects {
  this: TestSupportFixture =>

  lazy val audioVideoCSV @ audioVideoCSVRow1 :: audioVideoCSVRow2 :: Nil = List(
    Map(
      "SF_DOMAIN" -> "dans",
      "SF_USER" -> "janvanmansum",
      "SF_COLLECTION" -> "jans-test-files",
      "SF_PLAY_MODE" -> "menu",
      "AV_FILE_PATH" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ),
    Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> "",
      "AV_FILE_PATH" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur-nederlands.srt",
      "AV_SUBTITLES_LANGUAGE" -> "nl"
    )
  )

  lazy val audioVideoCSVRow = List(
    DepositRow(2, audioVideoCSVRow1),
    DepositRow(3, audioVideoCSVRow2),
  )

  lazy val audioVideo = AudioVideo(
    springfield = Option(Springfield("dans", "janvanmansum", "jans-test-files", PlayMode.Menu)),
    avFiles = Map(
      multiDepositDir / "ruimtereis01" / "reisverslag" / "centaur.mpg" ->
        Set(
          SubtitlesFile(
            file = multiDepositDir / "ruimtereis01" / "reisverslag" / "centaur.srt",
            language = Option("en")
          ),
          SubtitlesFile(
            file = multiDepositDir / "ruimtereis01" / "reisverslag" / "centaur-nederlands.srt",
            language = Option("nl")
          )
        )
    )
  )
}

class AudioVideoParserSpec extends TestSupportFixture with AudioVideoTestObjects with BeforeAndAfterEach {
  self =>

  override def beforeEach(): Unit = {
    super.beforeEach()

    if (multiDepositDir.exists) multiDepositDir.delete()
    File(getClass.getResource("/allfields/input").toURI).copyTo(multiDepositDir)
  }

  private val parser = new AudioVideoParser with ParserUtils with InputPathExplorer {
    val multiDepositDir: File = self.multiDepositDir
  }

  import parser._

  "extractAudioVideo" should "convert the csv input to the corresponding output" in {
    extractAudioVideo("ruimtereis01", 2, audioVideoCSVRow).validValue shouldBe audioVideo
  }

  it should "fail if there are AV_FILE_PATH values but there is no Springfield data" in {
    val rows = DepositRow(2, Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_ACCESSIBILITY" -> "NONE",
      "SF_PLAY_MODE" -> "",
      "AV_FILE_PATH" -> "",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )) :: DepositRow(3, audioVideoCSVRow2) :: Nil

    extractAudioVideo("ruimtereis01", 2, rows).invalidValue shouldBe
      ParseError(2, "The column 'AV_FILE_PATH' contains values, but the columns [SF_COLLECTION, SF_USER] do not").chained
  }

  it should "fail if there is more than one Springfield" in {
    // note: we require an extra column with the row numbers here!
    val rows = DepositRow(2, audioVideoCSVRow1) ::
      DepositRow(3, audioVideoCSVRow2.updated("SF_DOMAIN", "extra1")
        .updated("SF_USER", "extra2")
        .updated("SF_COLLECTION", "extra3")
        .updated("SF_PLAY_MODE", "continuous")) :: Nil

    extractAudioVideo("ruimtereis01", 2, rows).invalidValue shouldBe
      ParseError(2, "At most one row is allowed to contain a value for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION, SF_PLAY_MODE]. Found: [(dans,janvanmansum,jans-test-files,menu), (extra1,extra2,extra3,continuous)]").chained
  }

  "springfield" should "convert the csv input into the corresponding object" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> "menu"
    ))

    springfield(row).value.validValue shouldBe Springfield("randomdomain", "randomuser", "randomcollection", PlayMode.Menu)
  }

  it should "fail if the values have invalid characters" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "inv@lïdçhæracter",
      "SF_USER" -> "#%!&@$",
      "SF_COLLECTION" -> "inv***d",
      "SF_PLAY_MODE" -> "menu"
    ))

    springfield(row).value.invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "The column 'SF_DOMAIN' contains the following invalid characters: {@, ï, ç, æ}"),
      ParseError(2, "The column 'SF_USER' contains the following invalid characters: {#, %, !, &, @, $}"),
      ParseError(2, "The column 'SF_COLLECTION' contains the following invalid characters: {*}"),
    )
  }

  it should "convert with a default value for SF_DOMAIN when it is not defined" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> "menu"
    ))

    springfield(row).value.validValue shouldBe Springfield("dans", "randomuser", "randomcollection", PlayMode.Menu)
  }

  it should "fail if the values have invalid characters when no SF_DOMAIN is given" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "#%!&@$",
      "SF_COLLECTION" -> "inv***d",
      "SF_PLAY_MODE" -> "menu"
    ))

    springfield(row).value.invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "The column 'SF_USER' contains the following invalid characters: {#, %, !, &, @, $}"),
      ParseError(2, "The column 'SF_COLLECTION' contains the following invalid characters: {*}"),
    )
  }

  it should "fail if SF_PLAY_MODE is given but this is an unknown value" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> "unknown"
    ))

    springfield(row).value.invalidValue shouldBe
      ParseError(2, "Value 'unknown' is not a valid play mode").chained
  }

  it should "fail if no SF_PLAY_MODE is given" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> ""
    ))

    springfield(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: SF_PLAY_MODE").chained
  }

  it should "fail if there is no value for SF_COLLECTION" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> "menu"
    ))

    springfield(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: SF_COLLECTION").chained
  }

  it should "fail if there is no value for SF_COLLECTION and the value for SF_PLAY_MODE is unknown" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> "unknown"
    ))

    springfield(row).value.invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "Missing value for: SF_COLLECTION"),
      ParseError(2, "Value 'unknown' is not a valid play mode"),
    )
  }

  it should "fail if both SF_COLLECTION and SF_PLAY_MODE are not given" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> ""
    ))

    springfield(row).value.invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "Missing value for: SF_COLLECTION"),
      ParseError(2, "Missing value for: SF_PLAY_MODE"),
    )
  }

  it should "fail if there is no value for SF_USER" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> "menu"
    ))

    springfield(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: SF_USER").chained
  }

  it should "fail if there is no value for SF_USER and the value for SF_PLAY_MODE is unknown" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> "unknown"
    ))

    springfield(row).value.invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "Missing value for: SF_USER"),
      ParseError(2, "Value 'unknown' is not a valid play mode"),
    )
  }

  it should "fail if both SF_USER and SF_PLAY_MODE are not given" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> ""
    ))

    springfield(row).value.invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "Missing value for: SF_USER"),
      ParseError(2, "Missing value for: SF_PLAY_MODE"),
    )
  }

  it should "fail if both SF_USER and SF_COLLECTION are not given" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> "menu"
    ))

    springfield(row).value.invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "Missing value for: SF_COLLECTION"),
      ParseError(2, "Missing value for: SF_USER"),
    )
  }

  it should "fail if both SF_USER and SF_COLLECTION are not given and the value for SF_PLAY_MODE is unknown" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> "unknown"
    ))

    springfield(row).value.invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "Missing value for: SF_COLLECTION"),
      ParseError(2, "Missing value for: SF_USER"),
      ParseError(2, "Value 'unknown' is not a valid play mode"),
    )
  }

  it should "return None if there is no value for any of these keys" in {
    val row = DepositRow(2, Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> ""
    ))

    springfield(row) shouldBe empty
  }

  "playMode" should "convert the value for SF_PLAY_MODE into the corresponding enum object" in {
    playMode(2)("menu").validValue shouldBe PlayMode.Menu
  }

  it should "return the correct PlayMode if I use all-uppercase in the SF_PLAY_MODE value" in {
    playMode(2)("CONTINUOUS").validValue shouldBe PlayMode.Continuous
  }

  it should "return the correct PlayMode if I use some weird casing in the SF_PLAY_MODE value" in {
    playMode(2)("mEnU").validValue shouldBe PlayMode.Menu
  }

  it should "fail if the SF_PLAY_MODE value does not correspond to an object in the enum" in {
    playMode(2)("unknown value").invalidValue shouldBe
      ParseError(2, "Value 'unknown value' is not a valid play mode").chained
  }

  "avFile" should "convert the csv input into the corresponding object" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ))

    val file = multiDepositDir / "ruimtereis01" / "reisverslag" / "centaur.mpg"
    val subtitles = SubtitlesFile(multiDepositDir / "ruimtereis01" / "reisverslag" / "centaur.srt", Some("en"))
    avFile("ruimtereis01")(row).value.validValue shouldBe(file, subtitles)
  }

  it should "fail if the value for AV_FILE_PATH represents a path that does not exist" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "path/to/file/that/does/not/exist.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ))

    avFile("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "AV_FILE_PATH does not represent a valid path").chained
  }

  it should "fail if the value for AV_FILE_PATH represents a folder" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "reisverslag/",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ))

    avFile("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "AV_FILE_PATH does not represent a valid path").chained
  }

  it should "fail if the value for AV_FILE_PATH represents a path that does not exist when AV_SUBTITLES is not defined" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "path/to/file/that/does/not/exist.mpg",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    ))

    avFile("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "AV_FILE_PATH does not represent a valid path").chained
  }

  it should "fail if the value for AV_FILE_PATH represents a folder when AV_SUBTITLES is not defined" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "reisverslag/",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    ))

    avFile("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "AV_FILE_PATH does not represent a valid path").chained
  }

  it should "fail if the value for AV_SUBTITLES represents a path that does not exist" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "path/to/file/that/does/not/exist.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ))

    avFile("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "AV_SUBTITLES does not represent a valid path").chained
  }

  it should "fail if the value for AV_SUBTITLES represents a folder" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ))

    avFile("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "AV_SUBTITLES does not represent a valid path").chained
  }

  it should "fail if the value for AV_SUBTITLES_LANGUAGE does not represent an ISO 639-1 language value" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "ac"
    ))

    avFile("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "AV_SUBTITLES_LANGUAGE 'ac' doesn't have a valid ISO 639-1 language value").chained
  }

  it should "fail if there is no AV_SUBTITLES value, but there is a AV_SUBTITLES_LANGUAGE" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ))

    avFile("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: 'en'").chained
  }

  it should "succeed if there is a value for AV_SUBTITLES, but no value for AV_SUBTITLES_LANGUAGE" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> ""
    ))

    val file = multiDepositDir / "ruimtereis01" / "reisverslag" / "centaur.mpg"
    val subtitles = SubtitlesFile(multiDepositDir / "ruimtereis01" / "reisverslag" / "centaur.srt")
    avFile("ruimtereis01")(row).value.validValue shouldBe(`file`, `subtitles`)
  }

  it should "succeed if there is no value for both AV_SUBTITLES and AV_SUBTITLES_LANGUAGE" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    ))

    avFile("ruimtereis01")(row) shouldBe empty
  }

  it should "fail if there is no value for AV_FILE_PATH, but the other two do have values" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ))

    avFile("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "No value is defined for AV_FILE_PATH, while some of [AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined").chained
  }

  it should "return None if all four values do not have any value" in {
    val row = DepositRow(2, Map(
      "AV_FILE_PATH" -> "",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    ))

    avFile("ruimtereis01")(row) shouldBe empty
  }

  "iso639_1Language" should "return true if the tag matches an ISO 639-1 language" in {
    isValidISO639_1Language("en") shouldBe true
  }

  it should "return false if the tag is too long" in {
    isValidISO639_1Language("eng") shouldBe false
  }

  it should "return false if the tag is too short" in {
    isValidISO639_1Language("e") shouldBe false
  }

  it should "return false if the tag does not match a Locale" in {
    isValidISO639_1Language("ac") shouldBe false
  }
}
