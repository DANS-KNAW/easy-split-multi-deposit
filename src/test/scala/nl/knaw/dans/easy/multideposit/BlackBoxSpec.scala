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
package nl.knaw.dans.easy.multideposit

import java.io.File
import javax.naming.directory.{ Attributes, BasicAttribute, BasicAttributes }

import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter

import scala.util.{ Failure, Success }

class BlackBoxSpec extends UnitSpec with BeforeAndAfter with MockFactory {

  private val allfields = new File(testDir, "md/allfields").getAbsoluteFile
  private val invalidCSV = new File(testDir, "md/invalidCSV").getAbsoluteFile

  before {
    new File(getClass.getResource("/allfields/input").toURI).copyDir(allfields)
    new File(getClass.getResource("/invalidCSV/input").toURI).copyDir(invalidCSV)
  }

  "allfields" should "succeed in transforming the input into a bag" in {
    // this test does not work on travis, because we don't know the group that we can use for this
    assume(System.getProperty("user.name") != "travis")

    val ldap = mock[Ldap]
    implicit val settings = Settings(
      multidepositDir = allfields,
      stagingDir = new File(testDir, "sd").getAbsoluteFile,
      outputDepositDir = new File(testDir, "od").getAbsoluteFile,
      datamanager = "easyadmin",
      depositPermissions = DepositPermissions("rwxrwx---", "admin"),
      ldap = ldap
    )
    val expectedOutputDir = new File(getClass.getResource("/allfields/output").toURI)

    def createDatamanagerAttributes: BasicAttributes = {
      new BasicAttributes() {
        put("dansState", "ACTIVE")
        put(new BasicAttribute("easyRoles") {
          add("USER")
          add("ARCHIVIST")
        })
        put("mail", "FILL.IN.YOUR@VALID-EMAIL.NL")
      }
    }

    (ldap.query(_: String)(_: Attributes => Attributes)) expects(settings.datamanager, *) returning Success(Seq(createDatamanagerAttributes))
    (ldap.query(_: String)(_: Attributes => Boolean)) expects("user001", *) repeat 3 returning Success(Seq(true))

    Main.run shouldBe a[Success[_]]

    for (bagName <- Seq("ruimtereis01", "ruimtereis02", "ruimtereis03")) {
      // TODO I'm not happy with this way of testing the content of each file, especially with ignoring specific lines,
      // but I'm in a hurry, so I'll think of a better way later
      val bag = new File(settings.outputDepositDir, s"allfields-$bagName/bag")
      val expBag = new File(expectedOutputDir, s"input-$bagName/bag")

      val bagInfo = new File(bag, "bag-info.txt")
      val expBagInfo = new File(expBag, "bag-info.txt")
      bagInfo.read().lines.toSeq should contain allElementsOf expBagInfo.read().lines.filterNot(_ contains "Bagging-Date").toSeq

      val bagit = new File(bag, "bagit.txt")
      val expBagit = new File(expBag, "bagit.txt")
      bagit.read().lines.toSeq should contain allElementsOf expBagit.read().lines.toSeq

      val manifest = new File(bag, "manifest-sha1.txt")
      val expManifest = new File(expBag, "manifest-sha1.txt")
      manifest.read().lines.toSeq should contain allElementsOf expManifest.read().lines.toSeq

      val tagManifest = new File(bag, "tagmanifest-sha1.txt")
      val expTagManifest = new File(expBag, "tagmanifest-sha1.txt")
      tagManifest.read().lines.toSeq should contain allElementsOf expTagManifest.read().lines.filterNot(_ contains "bag-info.txt").filterNot(_ contains "manifest-sha1.txt").toSeq

      val datasetXml = new File(bag, "metadata/dataset.xml")
      val expDatasetXml = new File(expBag, "metadata/dataset.xml")
      datasetXml.read().lines.filterNot(_ contains "ddm:available").mkString("\n") shouldBe expDatasetXml.read().lines.filterNot(_ contains "ddm:available").mkString("\n")

      val filesXml = new File(bag, "metadata/files.xml")
      val expFilesXml = new File(expBag, "metadata/files.xml")
      filesXml.read() shouldBe expFilesXml.read()

      val props = new File(settings.outputDepositDir, s"allfields-$bagName/deposit.properties")
      val expProps = new File(expectedOutputDir, s"input-$bagName/deposit.properties")
      props.read().lines.toSeq should contain allElementsOf expProps.read().lines.filterNot(_ startsWith "#").filterNot(_ contains "bag-store.bag-id").toSeq
    }
  }

  "invalidCSV" should "fail in the parser step and return a report of the errors" in {
    implicit val settings = Settings(
      multidepositDir = invalidCSV
    )

    inside(Main.run) {
      case Failure(ParserFailedException(report, _)) =>
        report.lines.toSeq should contain inOrder(
          "CSV failures:",
          s" - row 2: AV_FILE file '${ settings.multidepositDir.getAbsolutePath }/ruimtereis01/path/to/audiofile/that/does/not/exist.mp3' does not exist",
          " - row 2: Missing value for: SF_USER",
          " - row 2: Only one row is allowed to contain a value for the column 'DDM_ACCESSRIGHTS'. Found: [OPEN_ACCESS, GROUP_ACCESS]",
          " - row 2: DDM_CREATED value 'invalid-date' does not represent a date",
          " - row 2: Only one row is allowed to contain a value for the column 'DEPOSITOR_ID'. Found: [user001, invalid-user]",
          " - row 3: No value is defined for AV_FILE, while some of [AV_FILE_TITLE, AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined",
          " - row 3: DDM_AVAILABLE value 'invalid-date' does not represent a date",
          "Due to these errors in the 'instructions.csv', nothing was done."
        )
    }
  }
}
