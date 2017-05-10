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

import java.io.File

import nl.knaw.dans.easy.multideposit.model.{ AVFile, AudioVideo, FileAccessRights, Springfield, Subtitles }
import nl.knaw.dans.easy.multideposit.{ ParseException, Settings, UnitSpec, _ }
import nl.knaw.dans.lib.error.CompositeException

import scala.util.{ Failure, Success }

trait AudioVideoTestObjects {

  val settings: Settings

  lazy val audioVideoCSV @ audioVideoCSVRow1 :: audioVideoCSVRow2 :: audioVideoCSVRow3 :: Nil = List(
    Map(
      "SF_DOMAIN" -> "dans",
      "SF_USER" -> "janvanmansum",
      "SF_COLLECTION" -> "jans-test-files",
      "SF_ACCESSIBILITY" -> "NONE",
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "video about the centaur meteorite",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ),
    Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_ACCESSIBILITY" -> "",
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "",
      "AV_SUBTITLES" -> "reisverslag/centaur-nederlands.srt",
      "AV_SUBTITLES_LANGUAGE" -> "nl"
    ),
    Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_ACCESSIBILITY" -> "",
      "AV_FILE" -> "path/to/a/random/sound/chicken.mp3",
      "AV_FILE_TITLE" -> "our daily wake up call",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )
  )

  lazy val audioVideo = AudioVideo(
    springfield = Option(Springfield("dans", "janvanmansum", "jans-test-files")),
    accessibility = Option(FileAccessRights.NONE),
    avFiles = Set(
      AVFile(
        file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile,
        title = Option("video about the centaur meteorite"),
        subtitles = List(
          Subtitles(
            file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile,
            language = Option("en")
          ),
          Subtitles(
            file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur-nederlands.srt").getAbsoluteFile,
            language = Option("nl")
          )
        )
      ),
      AVFile(
        file = new File(settings.multidepositDir, "ruimtereis01/path/to/a/random/sound/chicken.mp3").getAbsoluteFile,
        title = Option("our daily wake up call")
      )
    )
  )
}

class AudioVideoParserSpec extends UnitSpec with AudioVideoTestObjects { self =>

  override def beforeAll(): Unit = {
    super.beforeAll()
    new File(getClass.getResource("/allfields/input").toURI).copyDir(settings.multidepositDir)
  }

  override implicit val settings = Settings(
    multidepositDir = new File(testDir, "md").getAbsoluteFile
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
      "AV_FILE" -> "",
      "AV_FILE_TITLE" -> "",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    ) :: audioVideoCSVRow2 :: audioVideoCSVRow3 :: Nil

    extractAudioVideo(rows, 2, "ruimtereis01") should matchPattern {
      case Failure(ParseException(2, "The column 'AV_FILE' contains values, but the columns [SF_COLLECTION, SF_USER] do not", _)) =>
    }
  }

  it should "fail if there is more than one Springfield" in {
    val rows = audioVideoCSVRow1 ::
      audioVideoCSVRow2.updated("SF_DOMAIN", "extra1")
        .updated("SF_USER", "extra2")
        .updated("SF_COLLECTION", "extra3") ::
      audioVideoCSVRow3 :: Nil

    extractAudioVideo(rows, 2, "ruimtereis01") should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]. Found: [(dans,janvanmansum,jans-test-files), (extra1,extra2,extra3)]", _)) =>
    }
  }

  it should "fail if there is more than one file accessright" in {
    val rows = audioVideoCSVRow1 ::
      audioVideoCSVRow2.updated("SF_ACCESSIBILITY", "KNOWN") ::
      audioVideoCSVRow3 :: Nil

    extractAudioVideo(rows, 2, "ruimtereis01") should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for the column 'SF_ACCESSIBILITY'. Found: [NONE, KNOWN]", _)) =>
    }
  }

  it should "fail if there there are multiple AV_FILE_TITLEs for one file" in {
    val rows = audioVideoCSVRow1 ::
      audioVideoCSVRow2.updated("AV_FILE_TITLE", "another title") ::
      audioVideoCSVRow3 :: Nil

    inside(extractAudioVideo(rows, 2, "ruimtereis01")) {
      case Failure(CompositeException(es)) =>
        val ParseException(2, msg, _) :: Nil = es.toList
        val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
        msg shouldBe s"The column 'AV_FILE_TITLE' can only have one value for file '$file'"
    }
  }

  "springfield" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection"
    )

    springfield(2)(row).value should matchPattern {
      case Success(Springfield("randomdomain", "randomuser", "randomcollection")) =>
    }
  }

  it should "convert with a default value for SF_DOMAIN when it is not defined" in {
    val row = Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection"
    )

    springfield(2)(row).value should matchPattern {
      case Success(Springfield("dans", "randomuser", "randomcollection")) =>
    }
  }

  it should "fail if there is no value for SF_USER" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "randomcollection"
    )

    springfield(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: SF_USER", _)) =>
    }
  }

  it should "fail if there is no value for SF_COLLECTION" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> ""
    )

    springfield(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: SF_COLLECTION", _)) =>
    }
  }

  it should "fail if the values have invalid characters" in {
    val row = Map(
      "SF_DOMAIN" -> "inv@lïdçhæracter",
      "SF_USER" -> "#%!&@$",
      "SF_COLLECTION" -> "inv***d"
    )

    inside(springfield(2)(row).value) {
      case Failure(CompositeException(es)) =>
        val e1 :: e2 :: e3 :: Nil = es.toList
        e1 should have message "The column 'SF_DOMAIN' contains the following invalid characters: {@, ï, ç, æ}"
        e2 should have message "The column 'SF_USER' contains the following invalid characters: {#, %, !, &, @, $}"
        e3 should have message "The column 'SF_COLLECTION' contains the following invalid characters: {*}"
    }
  }

  it should "return None if there is no value for any of these keys" in {
    val row = Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> ""
    )

    springfield(2)(row) shouldBe empty
  }

  "avFile" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
    val title = Some("rolling stone")
    val subtitles = Some(Subtitles(new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile, Some("en")))
    avFile("ruimtereis01")(2)(row).value should matchPattern { case Success((`file`, `title`, `subtitles`)) => }
  }

  it should "succeed if the value for AV_FILE is relative to the multideposit rather than the deposit" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
    val title = Some("rolling stone")
    val subtitles = Some(Subtitles(new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile, Some("en")))
    avFile("ruimtereis01")(2)(row).value should matchPattern { case Success((`file`, `title`, `subtitles`)) => }
  }

  it should "succeed if the value for AV_SUBTITLES is relative to the multideposit rather than the deposit" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "ruimtereis01/reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
    val title = Some("rolling stone")
    val subtitles = Some(Subtitles(new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile, Some("en")))
    avFile("ruimtereis01")(2)(row).value should matchPattern { case Success((`file`, `title`, `subtitles`)) => }
  }

  it should "fail if the value for AV_FILE represents a path that does not exist" in {
    val row = Map(
      "AV_FILE" -> "path/to/file/that/does/not/exist.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
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
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/").getAbsoluteFile
    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe s"AV_FILE '$file' is not a file"
    }
  }

  it should "fail if the value for AV_FILE represents a path that does not exist when AV_SUBTITLES is not defined" in {
    val row = Map(
      "AV_FILE" -> "path/to/file/that/does/not/exist.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
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
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/").getAbsoluteFile
    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe s"AV_FILE '$file' is not a file"
    }
  }

  it should "fail if the value for AV_SUBTITLES represents a path that does not exist" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
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
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "reisverslag/",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/").getAbsoluteFile
    inside(avFile("ruimtereis01")(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe s"AV_SUBTITLES '$file' is not a file"
    }
  }

  it should "fail if the value for AV_SUBTITLES_LANGUAGE does not represent an ISO 639-1 language value" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
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
      "AV_FILE_TITLE" -> "rolling stone",
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
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
    val title = Some("rolling stone")
    val subtitles = Some(Subtitles(new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile))
    avFile("ruimtereis01")(2)(row).value should matchPattern { case Success((`file`, `title`, `subtitles`)) => }
  }

  it should "succeed if there is no value for both AV_SUBTITLES and AV_SUBTITLES_LANGUAGE" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
    val title = Some("rolling stone")
    avFile("ruimtereis01")(2)(row).value should matchPattern { case Success((`file`, `title`, None)) => }
  }

  it should "succeed if there is no value for AV_FILE_TITLE" in {
    val row = Map(
      "AV_FILE" -> "reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "",
      "AV_SUBTITLES" -> "reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
    val subtitles = Some(Subtitles(new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile))
    avFile("ruimtereis01")(2)(row).value should matchPattern { case Success((`file`, None, `subtitles`)) => }
  }

  it should "fail if there is no value for AV_FILE, but the other three do have values" in {
    val row = Map(
      "AV_FILE" -> "",
      "AV_FILE_TITLE" -> "rolling stone",
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
      "AV_FILE_TITLE" -> "",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    avFile("ruimtereis01")(2)(row) shouldBe empty
  }

  "fileAccessRight" should "convert the value for SF_ACCESSIBILITY into the corresponding enum object" in {
    val row = Map("SF_ACCESSIBILITY" -> "NONE")
    fileAccessRight(2)(row).value should matchPattern { case Success(FileAccessRights.NONE) => }
  }

  it should "return None if SF_ACCESSIBILITY is not defined" in {
    val row = Map("SF_ACCESSIBILITY" -> "")
    fileAccessRight(2)(row) shouldBe empty
  }

  it should "fail if the SF_ACCESSIBILITY value does not correspond to an object in the enum" in {
    val row = Map("SF_ACCESSIBILITY" -> "unknown value")
    fileAccessRight(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'unknown value' is not a valid file accessright", _)) =>
    }
  }
}
