/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.ps

import java.util.concurrent.TimeUnit

import org.apache.commons.io.FileUtils._
import org.scalatest.matchers.Matcher
import rx.lang.scala.Observable

import scala.concurrent.TimeoutException
import scala.concurrent.duration.Duration
import scala.util.{Try, Success}
import nl.knaw.dans.easy.ps.CustomMatchers._
import language.postfixOps
import nl.knaw.dans.easy.ps.AddEmdToDatasetIngestDirSpec.ActionExtension

class AddEmdToDatasetIngestDirSpec extends UnitSpec {

  implicit val s = Settings(ebiuDir = file(testDir,"ebiu"))

  def enoughEmd = dataset(Map( "DC_TITLE" -> List("Just a test")
    , "DC_DESCRIPTION" -> List("about mandatory DDM fields")
    , "DCX_CREATOR_INITIALS" -> List("A.")
    , "DCX_CREATOR_SURNAME" -> List("Developer")
    , "DDM_CREATED" -> List("2014")
    , "DDM_AUDIENCE" -> List("D32000")
    , "DDM_ACCESSRIGHTS" -> List("OPEN_ACCESS")
  ))

  /*
   * Design problem: the easiest way to validate the metadata is to use the schema validation against ddm.xsd. However,
   * the error messages coming from this validation will need mental mapping onto the columns of the SIP Instructions
   * file. Implementing validation directly on the columns on the other hand seems a violation of the single point of
   * definition principle.
   */
  "checkPreconditions" should "succeed if required EMD data is provided" in {

    val action = AddEmdToDatasetIngestDir("1", enoughEmd)
    action checkPreconditionsShould be(a[Success[_]])
  }

  it should "succeeed with just an organisation as creator" in {

    AddEmdToDatasetIngestDir("1", {enoughEmd -
        "DCX_CREATOR_INITIALS" -
        "DCX_CREATOR_SURNAME" +=
        ("DCX_CREATOR_ORGANIZATION" -> List("dans"))
    }) checkPreconditionsShould be(a[Success[_]])
  }

it should "fail if person creators and organization-only creators are mixed" in {

    AddEmdToDatasetIngestDir("1", {enoughEmd +=
      ("DCX_CREATOR_ORGANIZATION" -> List("dans"))
    }) checkPreconditionsShould be(a[Success[_]])
  }

  it should "fail if DC_DESCRIPTION is missing" in {

    AddEmdToDatasetIngestDir("1", enoughEmd - "DC_DESCRIPTION"
    ) checkPreconditionsShould failWithActionExceptionMatching (row = "1",msg = ".*:creator.*is expected.*")
  }

  it should "fail if creator has only last name" in {

    AddEmdToDatasetIngestDir("1", enoughEmd - "DCX_CREATOR_INITIALS"
    ) checkPreconditionsShould failWithActionExceptionMatching (row = "1",msg = ".*'dcx-dai:author' is not complete.*:initials.*is expected.*")
  }

  it should "fail if DC_TITLE is missing" in {

    AddEmdToDatasetIngestDir("1", enoughEmd - "DC_TITLE"
    ) checkPreconditionsShould failWithActionExceptionMatching (row = "1",msg = ".*:title.*is expected.*")
  }

  it should "fail if DDM_CREATED is missing" in {

    AddEmdToDatasetIngestDir("1", enoughEmd - "DDM_CREATED"
    ) checkPreconditionsShould failWithActionExceptionMatching (row = "1",msg = ".*:created.*is expected.*")
  }

  it should "fail if DDM_AUDIENCE is missing" in {

    AddEmdToDatasetIngestDir("1", enoughEmd - "DDM_AUDIENCE"
    ) checkPreconditionsShould failWithActionExceptionMatching (row = "1",msg = ".*:audience.*is expected.*")
  }

  it should "fail if DDM_ACCESSRIGHTS is missing" in {

    AddEmdToDatasetIngestDir("1", enoughEmd - "DDM_ACCESSRIGHTS"
    ) checkPreconditionsShould failWithActionExceptionMatching(row = "1", msg = ".*:accessRights.*is expected.*")
  }

  it should "fail without metadata in instructions" in {

    AddEmdToDatasetIngestDir("1", dataset(Map("DATASET_ID" -> List("dataset-1")))
    ) checkPreconditionsShould failWithActionExceptionMatching(row = "1", msg = ".*The content.*is not complete.*")
  }

  "run" should "succeed without metadata in instructions" in {
    AddEmdToDatasetIngestDir("1", dataset(Map("DATASET_ID" -> List("dataset-1"))))
      .run should be(a[Success[_]])
    file(s.ebiuDir, "dataset-1", EBIU_METADATA_DIR, EBIU_EMD_FILE) should be a 'file
  }

  it should "succeed if metadatafolder exists" in {

    val metadataDir = file(s.ebiuDir, "dataset-1", EBIU_METADATA_DIR)
    metadataDir.mkdirs()
    AddEmdToDatasetIngestDir("1", enoughEmd += ("DATASET_ID" -> List("dataset-1")))
      .run should be(a[Success[_]])
    file(metadataDir, EBIU_EMD_FILE) should be a 'file
  }

  it should "succeed if ebiuDir does not exist (path is created implicitely)" in {
    AddEmdToDatasetIngestDir("1", enoughEmd += ("DATASET_ID" -> List("dataset-1")))
      .run should be(a[Success[_]])
    file(s.ebiuDir, "dataset-1", EBIU_METADATA_DIR, EBIU_EMD_FILE) should be a 'file
  }

  it should "succeed if ebiuDir is not specified (working dir becomes ebiuDir)" in {
    implicit val s = Settings()// no ebiuDir: an exception to the other tests
    AddEmdToDatasetIngestDir("1", enoughEmd += ("DATASET_ID" -> List("dataset-1")))
      .run should be(a[Success[_]])
    file(s.ebiuDir, "dataset-1", EBIU_METADATA_DIR, EBIU_EMD_FILE) should be a 'file
    deleteQuietly(file(s.ebiuDir, "dataset-1"))// cleanup the mess
  }

  it should "fail if metadata folder could not be created (because it is a file)" in {
    write(file(s.ebiuDir, "dataset-1", EBIU_METADATA_DIR),"")
    AddEmdToDatasetIngestDir("1", enoughEmd += ("DATASET_ID" -> List("dataset-1")))
      .run should failWithActionExceptionMatching(row = "1", msg = ".*could not be created.*")
  }

  it should "fail if emdfile is not writable" in {
    val metadataFile = file(s.ebiuDir, "dataset-1", EBIU_METADATA_DIR, EBIU_EMD_FILE)
    write(metadataFile,"")
    metadataFile.setReadOnly()
    AddEmdToDatasetIngestDir("1", enoughEmd += ("DATASET_ID" -> List("dataset-1")))
      .run should failWithActionExceptionMatching(row = "1", msg = ".*cannot be written to.*")
  }

  "rollback" should "succeed after a succesful run, but it does not delete created emd file" in {
    val metadataDir = file(s.ebiuDir, "dataset-1", EBIU_METADATA_DIR)
    val action = AddEmdToDatasetIngestDir("1", enoughEmd += ("DATASET_ID" -> List("dataset-1")))
    action.run

    action.rollback() should be(a[Success[_]])
    file(metadataDir, EBIU_EMD_FILE) should be a 'file
  }
}

object AddEmdToDatasetIngestDirSpec {

  implicit class ActionExtension(val action: Action) extends UnitSpec {

    /** Ignores the test of checkPreconditions if XSD's are not available  */
    def checkPreconditionsShould(matchMe: Matcher[Try[Unit]]): Unit = {
      val webIsAvailable = try {
        val connection = DDM_SCHEMA_URL.openConnection()
        connection.setConnectTimeout(2000)
        connection.setReadTimeout(2000)
        connection.getInputStream
        true
      } catch {
        // without a connection, validation results in a SAXParseException "Failed to read schema document"
        case _: Exception => false
      }
      assume(webIsAvailable, s"no web connection to read $DDM_SCHEMA_URL")
      try {
        val timeoutPeriod = Duration(3, TimeUnit.SECONDS)
        Observable.just(action.checkPreconditions).timeout(timeoutPeriod).toBlocking.first should matchMe
      } catch {
        case _: TimeoutException => assume(false, s"slow 3rd party schemas used by $DDM_SCHEMA_URL: " +
          "http://dublincore.org/schemas/xmls/qdc/dc.xsd " +
          "http://dublincore.org/schemas/xmls/qdc/dcterms.xsd " +
          "http://dublincore.org/schemas/xmls/qdc/dcmitype.xsd " +
          "http://www.w3.org/2001/03/xml.xsd")
      }
    }
  }
}
