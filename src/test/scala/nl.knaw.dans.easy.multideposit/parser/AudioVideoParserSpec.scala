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

import java.nio.file.Paths

import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.easy.multideposit.{ ParseException, Settings, UnitSpec, _ }
import nl.knaw.dans.lib.error.CompositeException
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success }

trait AudioVideoTestObjects {

  val settings: Settings

  lazy val audioVideoCSV @ audioVideoCSVRow1 :: audioVideoCSVRow2 :: Nil = List(
    Map(
      "SF_DOMAIN" -> "dans",
      "SF_USER" -> "janvanmansum",
      "SF_COLLECTION" -> "jans-test-files",
      "SF_PLAY_MODE" -> "menu",
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ),
    Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> "",
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur-nederlands.srt",
      "AV_SUBTITLES_LANGUAGE" -> "nl"
    )
  )

  lazy val audioVideo = AudioVideo(
    springfield = Option(Springfield("dans", "janvanmansum", "jans-test-files", PlayMode.Menu)),
    avFiles = Map(
        settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.mpg").toAbsolutePath ->
          Set(
            Subtitles(
              path = settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.srt").toAbsolutePath,
              language = Option("en")
            ),
            Subtitles(
              path = settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur-nederlands.srt").toAbsolutePath,
              language = Option("nl")
            )
          )
    )
  )
}

class AudioVideoParserSpec extends UnitSpec with AudioVideoTestObjects with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    Paths.get(getClass.getResource("/allfields/input").toURI).copyDir(settings.multidepositDir)
  }

  override implicit val settings: Settings = Settings(
    multidepositDir = testDir.resolve("md").toAbsolutePath
  )
  private val parser = AudioVideoParser()

  import parser._

  "extractAudioVideo" should "convert the csv input to the corresponding output" in {
    extractAudioVideo(audioVideoCSV, 2, "ruimtereis01") should matchPattern { case Success(`audioVideo`) => }
  }

  it should "fail if there are AV_FILE values but there is no Springfield data" in {
    val rows = Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_ACCESSIBILITY" -> "NONE",
      "SF_PLAY_MODE" -> "",
      "AV_FILE" -> "",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    ) :: audioVideoCSVRow2 :: Nil

    extractAudioVideo(rows, 2, "ruimtereis01") should matchPattern {
      case Failure(ParseException(2, "The column 'AV_FILE' contains values, but the columns [SF_COLLECTION, SF_USER] do not", _)) =>
    }
  }

  it should "fail if there is more than one Springfield" in {
    // note: we require an extra column with the row numbers here!
    val rows = (audioVideoCSVRow1 + ("ROW" -> "1")) ::
      (audioVideoCSVRow2.updated("SF_DOMAIN", "extra1")
        .updated("SF_USER", "extra2")
        .updated("SF_COLLECTION", "extra3")
        .updated("SF_PLAY_MODE", "continuous") + ("ROW" -> "2")) :: Nil

    extractAudioVideo(rows, 2, "ruimtereis01") should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION, SF_PLAY_MODE]. Found: [(dans,janvanmansum,jans-test-files,menu), (extra1,extra2,extra3,continuous)]", _)) =>
    }
  }

  "springfield" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> "menu"
    )

    springfield(2)(row).value should matchPattern {
      case Success(Springfield("randomdomain", "randomuser", "randomcollection", PlayMode.Menu)) =>
    }
  }

  it should "convert with a default value for SF_DOMAIN when it is not defined" in {
    val row = Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> "menu"
    )

    springfield(2)(row).value should matchPattern {
      case Success(Springfield("dans", "randomuser", "randomcollection", PlayMode.Menu)) =>
    }
  }

  it should "fail if there is no value for SF_USER" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> "menu"
    )

    springfield(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: SF_USER", _)) =>
    }
  }

  it should "fail if there is no value for SF_USER and the value for SF_PLAY_MODE is unknown" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> "unknown"
    )

    springfield(2)(row).value should matchPattern {
      case Failure(CompositeException(ParseException(2, "Missing value for: SF_USER", _) :: ParseException(2, "Value 'unknown' is not a valid play mode", _) :: Nil)) =>
    }
  }

  it should "fail if there is no value for SF_COLLECTION" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> "menu"
    )

    springfield(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: SF_COLLECTION", _)) =>
    }
  }

  it should "fail if there is no value for SF_COLLECTION and the value for SF_PLAY_MODE is unknown" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> "unknown"
    )

    springfield(2)(row).value should matchPattern {
      case Failure(CompositeException(ParseException(2, "Missing value for: SF_COLLECTION", _) :: ParseException(2, "Value 'unknown' is not a valid play mode", _) :: Nil)) =>
    }
  }

  it should "fail if the values have invalid characters" in {
    val row = Map(
      "SF_DOMAIN" -> "inv@lïdçhæracter",
      "SF_USER" -> "#%!&@$",
      "SF_COLLECTION" -> "inv***d",
      "SF_PLAY_MODE" -> "menu"
    )

    inside(springfield(2)(row).value) {
      case Failure(CompositeException(e1 :: e2 :: e3 :: Nil)) =>
        e1 should have message "The column 'SF_DOMAIN' contains the following invalid characters: {@, ï, ç, æ}"
        e2 should have message "The column 'SF_USER' contains the following invalid characters: {#, %, !, &, @, $}"
        e3 should have message "The column 'SF_COLLECTION' contains the following invalid characters: {*}"
    }
  }

  it should "return None if there is no value for any of these keys" in {
    val row = Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> ""
    )

    springfield(2)(row) shouldBe empty
  }

  it should "fail if only the SF_PLAY_MODE is given" in {
    val row = Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> "menu"
    )

    springfield(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing values for these columns: [SF_COLLECTION, SF_USER]", _)) =>
    }
  }

  it should "default the SF_PLAY_MODE to 'continuous' when no value is given, but the Springfield fields are present" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection",
      "SF_PLAY_MODE" -> ""
    )

    springfield(2)(row).value should matchPattern {
      case Success(Springfield(_, _, _, PlayMode.Continuous)) =>
    }
  }

  it should "fail if only the SF_PLAY_MODE is given but this is an unknown value" in {
    val row = Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_PLAY_MODE" -> "unknown"
    )

    springfield(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'unknown' is not a valid play mode", _)) =>
    }
  }

  "playMode" should "convert the value for SF_PLAY_MODE into the corresponding enum object" in {
    val row = Map("SF_PLAY_MODE" -> "menu")
    playMode(2)(row).value should matchPattern { case Success(PlayMode.Menu) => }
  }

  it should "return the correct PlayMode if I use all-uppercase in the SF_PLAY_MODE value" in {
    val row = Map("SF_PLAY_MODE" -> "CONTINUOUS")
    playMode(2)(row).value should matchPattern { case Success(PlayMode.Continuous) => }
  }

  it should "return the correct PlayMode if I use some weird casing in the SF_PLAY_MODE value" in {
    val row = Map("SF_PLAY_MODE" -> "mEnU")
    playMode(2)(row).value should matchPattern { case Success(PlayMode.Menu) => }
  }

  it should "return None if SF_PLAY_MODE is not defined" in {
    val row = Map("SF_PLAY_MODE" -> "")
    playMode(2)(row) shouldBe empty
  }

  it should "fail if the SF_PLAY_MODE value does not correspond to an object in the enum" in {
    val row = Map("SF_PLAY_MODE" -> "unknown value")
    playMode(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'unknown value' is not a valid play mode", _)) =>
    }
  }

  "avFile" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.mpg").toAbsolutePath
    val subtitles = Subtitles(settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.srt").toAbsolutePath, Some("en"))
    avFile("ruimtereis01")(2)(row).value should matchPattern { case Success((`file`, `subtitles`)) => }
  }

  it should "succeed if the value for AV_FILE is relative to the multideposit rather than the deposit" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.mpg").toAbsolutePath
    val subtitles = Subtitles(settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.srt").toAbsolutePath, Some("en"))
    avFile("ruimtereis01")(2)(row).value should matchPattern { case Success((`file`, `subtitles`)) => }
  }

  it should "succeed if the value for AV_SUBTITLES is relative to the multideposit rather than the deposit" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "ruimtereis01/reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.mpg").toAbsolutePath
    val subtitles = Subtitles(settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.srt").toAbsolutePath, Some("en"))
    avFile("ruimtereis01")(2)(row).value should matchPattern { case Success((`file`, `subtitles`)) => }
  }

  it should "fail if the value for AV_FILE represents a path that does not exist" in {
    val row = Map(
      "AV_FILE" -> "path/to/file/that/does/not/exist.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe "AV_FILE 'path/to/file/that/does/not/exist.mpg' does not exist"
    }
  }

  it should "fail if the value for AV_FILE represents a folder" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = settings.multidepositDir.resolve("ruimtereis01/reisverslag/").toAbsolutePath
    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) => msg shouldBe s"AV_FILE '$file' is not a file"
    }
  }

  it should "fail if the value for AV_FILE represents a path that does not exist when AV_SUBTITLES is not defined" in {
    val row = Map(
      "AV_FILE" -> "path/to/file/that/does/not/exist.mpg",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe "AV_FILE 'path/to/file/that/does/not/exist.mpg' does not exist"
    }
  }

  it should "fail if the value for AV_FILE represents a folder when AV_SUBTITLES is not defined" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    val file = settings.multidepositDir.resolve("ruimtereis01/reisverslag/").toAbsolutePath
    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) => msg shouldBe s"AV_FILE '$file' is not a file"
    }
  }

  it should "fail if the value for AV_SUBTITLES represents a path that does not exist" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "path/to/file/that/does/not/exist.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe "AV_SUBTITLES 'path/to/file/that/does/not/exist.srt' does not exist"
    }
  }

  it should "fail if the value for AV_SUBTITLES represents a folder" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = settings.multidepositDir.resolve("ruimtereis01/reisverslag/").toAbsolutePath
    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) => msg shouldBe s"AV_SUBTITLES '$file' is not a file"
    }
  }

  it should "fail if the value for AV_SUBTITLES_LANGUAGE does not represent an ISO 639-1 language value" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "ac"
    )

    avFile("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(ParseException(2, "AV_SUBTITLES_LANGUAGE 'ac' doesn't have a valid ISO 639-1 language value", _)) =>
    }
  }

  it should "fail if there is no AV_SUBTITLES value, but there is a AV_SUBTITLES_LANGUAGE" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe s"Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: 'en'"
    }
  }

  it should "succeed if there is a value for AV_SUBTITLES, but no value for AV_SUBTITLES_LANGUAGE" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    val file = settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.mpg").toAbsolutePath
    val subtitles = Subtitles(settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.srt").toAbsolutePath)
    avFile("ruimtereis01")(2)(row).value should matchPattern { case Success((`file`, `subtitles`)) => }
  }

  it should "succeed if there is no value for both AV_SUBTITLES and AV_SUBTITLES_LANGUAGE" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    avFile("ruimtereis01")(2)(row) shouldBe empty
  }

  it should "fail if there is no value for AV_FILE, but the other two do have values" in {
    val row = Map(
      "AV_FILE" -> "",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg should include("No value is defined for AV_FILE")
    }
  }

  it should "return None if all four values do not have any value" in {
    val row = Map(
      "AV_FILE" -> "",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    avFile("ruimtereis01")(2)(row) shouldBe empty
  }
}
