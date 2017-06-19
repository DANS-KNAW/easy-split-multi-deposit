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

import java.nio.file.Paths
import javax.naming.directory.{ Attributes, BasicAttribute, BasicAttributes }

import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter

import scala.util.{ Failure, Success }
import scala.xml.transform.{ RewriteRule, RuleTransformer }
import scala.xml.{ Elem, Node, NodeSeq, XML }

class BlackBoxSpec extends UnitSpec with BeforeAndAfter with MockFactory with CustomMatchers {

  private val allfields = testDir.resolve("md/allfields").toAbsolutePath
  private val invalidCSV = testDir.resolve("md/invalidCSV").toAbsolutePath

  before {
    Paths.get(getClass.getResource("/allfields/input").toURI).copyDir(allfields)
    Paths.get(getClass.getResource("/invalidCSV/input").toURI).copyDir(invalidCSV)
  }

  "allfields" should "succeed in transforming the input into a bag" in {
    // this test does not work on travis, because we don't know the group that we can use for this
    assume(System.getProperty("user.name") != "travis")

    val ldap = mock[Ldap]
    implicit val settings = Settings(
      multidepositDir = allfields,
      stagingDir = testDir.resolve("sd").toAbsolutePath,
      outputDepositDir = testDir.resolve("od").toAbsolutePath,
      datamanager = "easyadmin",
      depositPermissions = DepositPermissions("rwxrwx---", "admin"),
      formatsFile = formatsFile,
      ldap = ldap
    )
    val expectedOutputDir = Paths.get(getClass.getResource("/allfields/output").toURI)

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
    (ldap.query(_: String)(_: Attributes => Boolean)) expects("user001", *) repeat 4 returning Success(Seq(true))

    Main.run shouldBe a[Success[_]]

    for (bagName <- Seq("ruimtereis01", "ruimtereis02", "ruimtereis03", "ruimtereis04")) {
      // TODO I'm not happy with this way of testing the content of each file, especially with ignoring specific lines,
      // but I'm in a hurry, so I'll think of a better way later
      val bag = settings.outputDepositDir.resolve(s"allfields-$bagName/bag")
      val expBag = expectedOutputDir.resolve(s"input-$bagName/bag")

      val bagInfo = bag.resolve("bag-info.txt")
      val expBagInfo = expBag.resolve("bag-info.txt")
      bagInfo.read().lines.toSeq should contain allElementsOf expBagInfo.read().lines.filterNot(_ contains "Bagging-Date").toSeq

      val bagit = bag.resolve("bagit.txt")
      val expBagit = expBag.resolve("bagit.txt")
      bagit.read().lines.toSeq should contain allElementsOf expBagit.read().lines.toSeq

      val manifest = bag.resolve("manifest-sha1.txt")
      val expManifest = expBag.resolve("manifest-sha1.txt")
      manifest.read().lines.toSeq should contain allElementsOf expManifest.read().lines.toSeq

      val tagManifest = bag.resolve("tagmanifest-sha1.txt")
      val expTagManifest = expBag.resolve("tagmanifest-sha1.txt")
      tagManifest.read().lines.toSeq should contain allElementsOf expTagManifest.read().lines.filterNot(_ contains "bag-info.txt").filterNot(_ contains "manifest-sha1.txt").toSeq

      val datasetXml = bag.resolve("metadata/dataset.xml")
      val expDatasetXml = expBag.resolve("metadata/dataset.xml")
      val datasetTransformer = removeElemByName("available")
      datasetTransformer.transform(XML.loadFile(datasetXml.toFile)) should equalTrimmed (datasetTransformer.transform(XML.loadFile(expDatasetXml.toFile)))

      val filesXml = bag.resolve("metadata/files.xml")
      val expFilesXml = expBag.resolve("metadata/files.xml")
      XML.loadFile(filesXml.toFile) should equalTrimmed (XML.loadFile(expFilesXml.toFile))

      val props = settings.outputDepositDir.resolve(s"allfields-$bagName/deposit.properties")
      val expProps = expectedOutputDir.resolve(s"input-$bagName/deposit.properties")
      props.read().lines.toSeq should contain allElementsOf expProps.read().lines.filterNot(_ startsWith "#").filterNot(_ contains "bag-store.bag-id").toSeq
    }
  }

  "invalidCSV" should "fail in the parser step and return a report of the errors" in {
    implicit val settings = Settings(
      multidepositDir = invalidCSV
    )

    inside(Main.run) {
      case Failure(ParserFailedException(report, _)) =>
        report.lines.toSeq should contain inOrderOnly(
          "CSV failures:",
          " - row 2: Only one row is allowed to contain a value for the column 'DEPOSITOR_ID'. Found: [user001, invalid-user]",
          " - row 2: DDM_CREATED value 'invalid-date' does not represent a date",
          " - row 2: Only one row is allowed to contain a value for the column 'DDM_ACCESSRIGHTS'. Found: [OPEN_ACCESS, GROUP_ACCESS]",
          " - row 2: Value 'random test data' is not a valid type",
          " - row 2: Value 'NL' is not a valid value for DC_LANGUAGE",
          " - row 2: Missing value for: SF_USER",
          " - row 2: AV_FILE 'path/to/audiofile/that/does/not/exist.mp3' does not exist",
          " - row 3: DDM_AVAILABLE value 'invalid-date' does not represent a date",
          " - row 3: Missing value for: DC_IDENTIFIER",
          " - row 3: Value 'encoding=UTF-8' is not a valid value for DC_LANGUAGE",
          " - row 3: No value is defined for AV_FILE, while some of [AV_FILE_TITLE, AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined",
          "Due to these errors in the 'instructions.csv', nothing was done."
        )
    }
  }

  def removeElemByName(label: String) = new RuleTransformer(new RewriteRule {
    override def transform(n: Node): Seq[Node] = {
      n match {
        case e: Elem if e.label == label => NodeSeq.Empty
        case e => e
      }
    }
  })
}
