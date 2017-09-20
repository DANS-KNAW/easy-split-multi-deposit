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

import java.nio.file.{ Files, Paths }
import javax.naming.directory.{ Attributes, BasicAttribute, BasicAttributes }

import org.scalamock.scalatest.MockFactory
import resource.managed

import scala.collection.JavaConverters._
import scala.util.{ Failure, Properties, Success }
import scala.xml.transform.{ RewriteRule, RuleTransformer }
import scala.xml.{ Elem, Node, NodeSeq, XML }

// Note to developers: this classes uses shared tests as described in
// http://www.scalatest.org/user_guide/sharing_tests
class BlackBoxSpec extends UnitSpec with MockFactory with CustomMatchers {

  private val formats = """
                          |application/postscript
                          |application/rtf
                          |application/pdf
                          |application/msword
                          |text/plain
                          |text/html
                          |text/sgml
                          |text/xml
                          |image/jpeg
                          |image/gif
                          |image/tiff
                          |video/quicktime
                          |video/mpeg1
                        """.stripMargin.split("\n").map(_.trim).toSet
  private val allfields = testDir.resolve("md/allfields").toAbsolutePath
  private val invalidCSV = testDir.resolve("md/invalidCSV").toAbsolutePath

  /*
    Note to future developers:
    We're sharing tests in this BlackBoxSpec to prevent as much test duplication as possible.
    However, a test like `"allfields" should behave like allfieldsSpec()`, combined with a
    `BeforeAndAfterAll` will always first execute the setup stuff from `allfieldsSpec()` and only
    then run the `beforeAll` and `afterAll` functions. Other traits like `BeforeAndAfter` and
    `BeforeAndAfterEach` are isomorphic.
    By not using these traits but manually defining and calling `beforeAll` we make sure this
    function is called before the shared tests are set up or ran.
   */
  def beforeAll(): Unit = {
    Paths.get(getClass.getResource("/allfields/input").toURI).copyDir(allfields)
    Paths.get(getClass.getResource("/invalidCSV/input").toURI).copyDir(invalidCSV)
  }

  private def doNotRunOnTravis() = {
    assume(System.getProperty("user.name") != "travis",
      "this test does not work on travis, because we don't know the group that we can use for this")
  }

  private lazy val getFileSystemGroup: String = {
    import scala.sys.process._

    s"id -Gn ${ Properties.userName }".!!
      .split(" ")
      .headOption
      .getOrElse(throw new AssertionError("no suitable user group found"))
  }

  def allfieldsSpec(): Unit = {
    val ldap = mock[Ldap]
    implicit val settings: Settings = Settings(
      multidepositDir = allfields,
      stagingDir = testDir.resolve("sd").toAbsolutePath,
      outputDepositDir = testDir.resolve("od").toAbsolutePath,
      datamanager = "easyadmin",
      depositPermissions = DepositPermissions("rwxrwx---", getFileSystemGroup),
      formats = formats,
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

    it should "succeed running the application" in {
      doNotRunOnTravis()

      (ldap.query(_: String)(_: Attributes => Attributes)) expects(settings.datamanager, *) returning Success(Seq(createDatamanagerAttributes))
      (ldap.query(_: String)(_: Attributes => Boolean)) expects("user001", *) repeat 4 returning Success(Seq(true))

      Main.run shouldBe a[Success[_]]
    }

    val expectedDataContentRuimtereis01 = Set("data/", "ruimtereis01_verklaring.txt", "path/",
      "to/", "a/", "random/", "video/", "hubble.mpg", "reisverslag/", "centaur.mpg", "centaur.srt",
      "centaur-nederlands.srt", "deel01.docx", "deel01.txt", "deel02.txt", "deel03.txt")
    val expectedDataContentRuimtereis02 = Set("data/", "hubble-wiki-en.txt", "hubble-wiki-nl.txt",
      "path/", "to/", "images/", "Hubble_01.jpg", "Hubbleshots.jpg")
    val expectedDataContentRuimtereis03 = Set("data/")
    val expectedDataContentRuimtereis04 = Set("data/", "Quicksort.hs", "path/", "to/", "a/",
      "random/", "file/", "file.txt", "sound/", "chicken.mp3")

    def bagContents(bagName: String, dataContent: Set[String]): Unit = {
      val bag = settings.outputDepositDir.resolve(s"allfields-$bagName/bag")
      val expBag = expectedOutputDir.resolve(s"input-$bagName/bag")

      it should "check the files present in the bag" in {
        doNotRunOnTravis()

        managed(Files.list(bag))
          .acquireAndGet(_.iterator().asScala.toList)
          .map(_.getFileName.toString) should contain only(
          "bag-info.txt",
          "bagit.txt",
          "manifest-sha1.txt",
          "tagmanifest-sha1.txt",
          "data",
          "metadata")
      }

      it should "check bag-info.txt" in {
        doNotRunOnTravis()

        val bagInfo = bag.resolve("bag-info.txt")
        val expBagInfo = expBag.resolve("bag-info.txt")

        // skipping the Bagging-Date which is different every time
        bagInfo.read().lines.toSeq should contain allElementsOf
          expBagInfo.read().lines.filterNot(_ contains "Bagging-Date").toSeq
      }

      it should "check bagit.txt" in {
        doNotRunOnTravis()

        val bagit = bag.resolve("bagit.txt")
        val expBagit = expBag.resolve("bagit.txt")

        bagit.read().lines.toSeq should contain allElementsOf expBagit.read().lines.toSeq
      }

      it should "check manifest-sha1.txt" in {
        doNotRunOnTravis()

        val manifest = bag.resolve("manifest-sha1.txt")
        val expManifest = expBag.resolve("manifest-sha1.txt")

        manifest.read().lines.toSeq should contain allElementsOf expManifest.read().lines.toSeq
      }

      it should "check tagmanifest-sha1.txt" in {
        doNotRunOnTravis()

        val tagManifest = bag.resolve("tagmanifest-sha1.txt")
        val expTagManifest = expBag.resolve("tagmanifest-sha1.txt")

        // skipping bag-info.txt and manifest-sha1.txt which are different every time
        // due to the Bagging-Date and 'available' in metadata/dataset.xml
        tagManifest.read().lines.toSeq should contain allElementsOf
          expTagManifest.read().lines
            .filterNot(_ contains "bag-info.txt")
            .filterNot(_ contains "manifest-sha1.txt").toSeq
      }

      it should "check the files in data/" in {
        doNotRunOnTravis()

        val dataDir = bag.resolve("data/")
        dataDir.toFile should exist
        dataDir.listRecursively().map {
          case file if Files.isDirectory(file) => file.getFileName.toString + "/"
          case file => file.getFileName.toString
        } should contain theSameElementsAs dataContent
      }

      it should "check the files in metadata/" in {
        doNotRunOnTravis()

        managed(Files.list(bag.resolve("metadata")))
          .acquireAndGet(_.iterator().asScala.toList)
          .map(_.getFileName.toString) should contain only("dataset.xml", "files.xml")
      }

      it should "check metadata/dataset.xml" in {
        doNotRunOnTravis()

        def removeElemByName(label: String) = new RuleTransformer(new RewriteRule {
          override def transform(n: Node): Seq[Node] = {
            n match {
              case e: Elem if e.label == label => NodeSeq.Empty
              case e => e
            }
          }
        })

        val datasetXml = XML.loadFile(bag.resolve("metadata/dataset.xml").toFile)
        val expDatasetXml = XML.loadFile(expBag.resolve("metadata/dataset.xml").toFile)
        val datasetTransformer = removeElemByName("available")

        // skipping the available field here
        datasetTransformer.transform(datasetXml) should
          equalTrimmed(datasetTransformer.transform(expDatasetXml))
      }

      // in this test we cannot compare the actual contents, since the order of the <file>
      // elements might differ per machine. Therefore we compare all the <file> elements separately.
      it should "check metadata/files.xml" in {
        doNotRunOnTravis()

        val filesXml = XML.loadFile(bag.resolve("metadata/files.xml").toFile)
        val expFilesXml = XML.loadFile(expBag.resolve("metadata/files.xml").toFile)

        val files = filesXml \ "file"
        val expFiles = expFilesXml \ "file"

        def getPath(node: Node): String = node \@ "filepath"

        // if we check the sizes first, we only have to compare in one way
        files.size shouldBe expFiles.size
        for (file <- files) {
          val path = getPath(file)
          expFiles.find(expectedFile => getPath(expectedFile) == path)
            .map(expectedFile => file should equalTrimmed(expectedFile))
            .getOrElse(fail(s"the expected output did not contain a <file> for $path"))
        }
      }

      it should "check deposit.properties" in {
        doNotRunOnTravis()

        val props = settings.outputDepositDir.resolve(s"allfields-$bagName/deposit.properties")
        val expProps = expectedOutputDir.resolve(s"input-$bagName/deposit.properties")

        // skipping comment lines, as well as the line with randomized bag-id
        props.read().lines.toSeq should contain allElementsOf
          expProps.read().lines
            .filterNot(_ startsWith "#")
            .filterNot(_ contains "bag-store.bag-id")
            .toSeq
      }
    }

    "ruimtereis01" should behave like bagContents("ruimtereis01", expectedDataContentRuimtereis01)
    "ruimtereis02" should behave like bagContents("ruimtereis02", expectedDataContentRuimtereis02)
    "ruimtereis03" should behave like bagContents("ruimtereis03", expectedDataContentRuimtereis03)
    "ruimtereis04" should behave like bagContents("ruimtereis04", expectedDataContentRuimtereis04)
  }

  beforeAll()

  "allfields" should behave like allfieldsSpec()

  "invalidCSV" should "fail in the parser step and return a report of the errors" in {
    implicit val settings: Settings = Settings(
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
          " - row 2: DCT_DATE value 'Text with Qualifier' does not represent a date",
          " - row 2: FILE_PATH 'path/to/audiofile/that/does/not/exist.mp3' does not exist",
          " - row 2: Missing value for: SF_USER",
          " - row 3: DDM_AVAILABLE value 'invalid-date' does not represent a date",
          " - row 3: Missing value for: DC_IDENTIFIER",
          " - row 3: Value 'encoding=UTF-8' is not a valid value for DC_LANGUAGE",
          " - row 3: DCT_DATE_QUALIFIER is only allowed to have a value if DCT_DATE has a well formatted date to go with it",
          " - row 3: FILE_TITLE is not allowed, since FILE_PATH isn't given",
          " - row 4: DCT_DATE value '30-07-1992' does not represent a date",
          "Due to these errors in the 'instructions.csv', nothing was done."
        )
    }
  }
}
