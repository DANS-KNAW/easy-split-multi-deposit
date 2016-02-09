package nl.knaw.dans.easy.multiDeposit

import java.io.File

import org.scalatest._

import scala.collection.mutable.ListBuffer

abstract class UnitSpec extends FlatSpec with Matchers with
OptionValues with Inside with Inspectors with OneInstancePerTest {

  val testDir = new File(s"target/test/${getClass.getSimpleName}")

  def testDataset1: Dataset = {
    val dataset = new Dataset
    dataset += "ROW" -> List("2", "3", "4")
    dataset += "DATASET_ID" -> List("ruimtereis01", "ruimtereis01", "ruimtereis01")
    dataset += "DDM_CREATED" -> List("2015-05-19", "", "")
    dataset += "DC_TITLE" -> List("Reis naar Centaur-planetoïde", "Trip to Centaur asteroid", "")
    dataset += "DC_DESCRIPTION" -> List("Een tweedaagse reis per ruimteschip naar een bijzondere planetoïde in de omgeving van Jupiter.", "A two day mission to boldly go where no man has gone before", "")
    dataset += "DDM_AUDIENCE" -> List("D30000", "", "")
    dataset += "DDM_ACCESSRIGHTS" -> List("OPEN_ACCESS", "", "")
    dataset += "DCX_CREATOR_TITLES" -> List("Captain", "", "")
    dataset += "DCX_CREATOR_INITIALS" -> List("J.T.", "", "")
    dataset += "DCX_CREATOR_SURNAME" -> List("Kirk", "", "")
    dataset += "DCX_CREATOR_ORGANIZATION" -> List("United Federation of Planets", "", "")
    dataset += "DC_SUBJECT" -> List("astronomie", "ruimtevaart", "planetoïden")
    dataset += "DC_FORMAT" -> List("video/mpeg", "text/plain", "")
    dataset += "DC_LANGUAGE" -> List("NL", "encoding=UTF-8", "")
    dataset += "SF_DOMAIN" -> List("dans", "", "")
    dataset += "SF_USER" -> List("janvanmansum", "", "")
    dataset += "SF_COLLECTION" -> List("Jans-test-files", "", "")
    dataset += "SF_PRESENTATION" -> List("centaur", "", "")
    dataset += "FILE_SIP" -> List("videos/centaur.mpg", "", "")
    dataset += "FILE_DATASET" -> List("footage/centaur.mpg", "", "")
    dataset += "FILE_STORAGE_SERVICE" -> List("http://zandbak11.dans.knaw.nl/webdav", "", "")
    dataset += "FILE_STORAGE_PATH" -> List("", "", "")
    dataset += "FILE_AUDIO_VIDEO" -> List("Yes", "", "")
    dataset += "FILE_SUBTITLES" -> List("", "", "")
  }

  def testDataset2: Dataset = {
    val dataset = new Dataset

    dataset += "ROW" -> List("2", "3", "4", "5", "6")
    dataset += "DATASET_ID" -> List("dataset-1", "dataset-1", "dataset-1", "dataset-1", "dataset-1")
    dataset += "DC_TITLE" -> List("Title 1 of dataset 1", "Title 2 of dataset 1", "", "", "")
    dataset += "DC_DESCRIPTION" -> List("A sample dataset with a not very long description", "", "", "", "")
    dataset += "DC_CREATOR" -> List("Creator A", "", "", "", "")
    dataset += "DC_CONTRIBUTOR" -> List("Contributor 1", "Contributor 2", "", "", "")
    dataset += "DCX_CREATOR_ORGANIZATION" -> List("", "", "", "", "")
    dataset += "DCX_CREATOR_DAI" -> List("", "", "", "", "")
    dataset += "DCX_CREATOR_SURNAME" -> List("", "", "", "", "")
    dataset += "DC_SUBJECT" -> List("subject 1", "subject 2", "", "subject 3", "")
    dataset += "DC_PUBLISHER" -> List("", "", "publisher 1", "", "")
    dataset += "DC_TYPE" -> List("type1", "", "", "", "")
    dataset += "DC_FORMAT" -> List("", "", "", "", "")
    dataset += "DC_IDENTIFIER" -> List("id1234", "", "", "", "")
    dataset += "DC_SOURCE" -> List("", "", "", "", "")
    dataset += "DC_LANGUAGE" -> List("", "", "", "", "")
    dataset += "SF_DOMAIN" -> List("dans", "", "", "", "")
    dataset += "SF_USER" -> List("project1", "", "", "", "")
    dataset += "SF_COLLECTION" -> List("coll1", "", "", "", "")
    dataset += "SF_PRESENTATION" -> List("presentation1", "", "", "", "")
    dataset += "FILE_SUBTITLES" -> List("", "", "", "", "")
    dataset += "FILE_SIP" -> List("dataset-1/no-default-processing.txt", "dataset-1/some/dir/path/no-default-processing-2.txt", "videos/centaur.mpg", "", "")
    dataset += "FILE_DATASET" -> List("some/other/dir/path/non-default-1.txt", "some/other/dir/path/non-default-2.txt", "", "", "")
    dataset += "FILE_STORAGE_SERVICE" -> List("", "", "", "", "")
    dataset += "FILE_STORAGE_PATH" -> List("", "", "", "", "")
    dataset += "FILE_AUDIO_VIDEO" -> List("", "", "Yes", "", "")
  }

  def testDatasets: Datasets = ListBuffer(("dataset-1", testDataset1), ("dataset-2", testDataset2))

  def testFileParameters1: List[FileParameters] = {
    FileParameters(Option("2"), Option("videos/centaur.mpg"), Option("footage/centaur.mpg"), Option("http://zandbak11.dans.knaw.nl/webdav"), None, Option("Yes")) ::
    Nil
  }

  def testFileParameters2: List[FileParameters] = {
    FileParameters(Some("2"), Some("dataset-1/no-default-processing.txt"), Some("some/other/dir/path/non-default-1.txt"), None, None, None) ::
    FileParameters(Some("3"), Some("dataset-1/some/dir/path/no-default-processing-2.txt"), Some("some/other/dir/path/non-default-2.txt"), None, None, None) ::
    FileParameters(Some("4"), Some("videos/centaur.mpg"), None, None, None, Some("Yes")) ::
    Nil
  }
}
