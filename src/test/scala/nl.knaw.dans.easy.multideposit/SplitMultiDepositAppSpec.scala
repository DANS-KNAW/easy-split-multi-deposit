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

import better.files.File
import better.files.File.currentWorkingDirectory
import javax.naming.directory.{ Attributes, BasicAttribute, BasicAttributes }
import nl.knaw.dans.easy.multideposit.PathExplorer.PathExplorers
import nl.knaw.dans.easy.multideposit.parser.ParserFailedException
import org.scalamock.scalatest.MockFactory

import scala.util.{ Failure, Properties, Success }
import scala.xml.transform.{ RewriteRule, RuleTransformer }
import scala.xml.{ Elem, Node, NodeSeq, XML }

// Note to developers: this classes uses shared tests as described in
// http://www.scalatest.org/user_guide/sharing_tests
class SplitMultiDepositAppSpec extends TestSupportFixture with MockFactory with CustomMatchers {

  private val formatsFile: File = currentWorkingDirectory / "src" / "main" / "assembly" / "dist" / "cfg" / "acceptedMediaTypes.txt"
  private val formats =
    if (formatsFile.exists) formatsFile.lines.map(_.trim).toSet
    else fail("Cannot find file: acceptedMediaTypes.txt")

  "acceptedMediaFiles" should "contain certain formats" in {
    formats should contain("audio/mpeg3")
  }

  private val allfields = testDir / "md" / "allfields"
  private val invalidCSV = testDir / "md" / "invalidCSV"

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
    File(getClass.getResource("/allfields/input").toURI).copyTo(allfields)
    File(getClass.getResource("/invalidCSV/input").toURI).copyTo(invalidCSV)
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
    val datamanager = "easyadmin"
    val paths = new PathExplorers(
      md = allfields,
      sd = testDir / "sd",
      od = (testDir / "od").createIfNotExists(asDirectory = true, createParents = true))
    val app = new SplitMultiDepositApp(formats, ldap, DepositPermissions("rwxrwx---", getFileSystemGroup))

    val expectedOutputDir = File(getClass.getResource("/allfields/output").toURI)

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

    def configureLdapMockBehavior() = {
      (ldap.query(_: String)(_: Attributes => Attributes)) expects(datamanager, *) returning Success(Seq(createDatamanagerAttributes))
      (ldap.query(_: String)(_: Attributes => Boolean)) expects("user001", *) repeat 4 returning Success(Seq(true))
    }

    it should "succeed validating the multideposit" in {
      configureLdapMockBehavior()
      app.validate(paths, datamanager) shouldBe a[Success[_]]
    }

    it should "succeed converting the multideposit" in {
      doNotRunOnTravis()
      configureLdapMockBehavior()
      app.convert(paths, datamanager) shouldBe a[Success[_]]
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
      val bag = paths.outputDepositDir / s"allfields-$bagName/bag"
      val expBag = expectedOutputDir / s"input-$bagName/bag"

      it should "check the files present in the bag" in {
        doNotRunOnTravis()

        bag.list.map(_.name).toList should contain only(
          "bag-info.txt",
          "bagit.txt",
          "manifest-sha1.txt",
          "tagmanifest-sha1.txt",
          "data",
          "metadata")
      }

      it should "check bag-info.txt" in {
        doNotRunOnTravis()

        val bagInfo = bag / "bag-info.txt"
        val expBagInfo = expBag / "bag-info.txt"

        // skipping the Bagging-Date which is different every time
        bagInfo.lines.toSeq should contain allElementsOf
          expBagInfo.lines.filterNot(_ contains "Bagging-Date").toSeq
      }

      it should "check bagit.txt" in {
        doNotRunOnTravis()

        val bagit = bag / "bagit.txt"
        val expBagit = expBag / "bagit.txt"

        bagit.lines.toSeq should contain allElementsOf expBagit.lines.toSeq
      }

      it should "check manifest-sha1.txt" in {
        doNotRunOnTravis()

        val manifest = bag / "manifest-sha1.txt"
        val expManifest = expBag / "manifest-sha1.txt"

        manifest.lines.toSeq should contain allElementsOf expManifest.lines.toSeq
      }

      it should "check tagmanifest-sha1.txt" in {
        doNotRunOnTravis()

        val tagManifest = bag / "tagmanifest-sha1.txt"
        val expTagManifest = expBag / "tagmanifest-sha1.txt"

        // skipping bag-info.txt and manifest-sha1.txt which are different every time
        // due to the Bagging-Date and 'available' in metadata/dataset.xml
        tagManifest.lines.toSeq should contain allElementsOf
          expTagManifest.lines
            .filterNot(_ contains "bag-info.txt")
            .filterNot(_ contains "manifest-sha1.txt").toSeq
      }

      it should "check the files in data/" in {
        doNotRunOnTravis()

        val dataDir = bag / "data/"
        dataDir.toJava should exist
        dataDir.walk().map {
          case file if file.isDirectory => file.name + "/"
          case file => file.name
        }.toList should contain theSameElementsAs dataContent
      }

      it should "check the files in metadata/" in {
        doNotRunOnTravis()

        (bag / "metadata").list.map(_.name).toList should contain only("dataset.xml", "files.xml")
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

        val datasetXml = XML.loadFile((bag / "metadata" / "dataset.xml").toJava)
        val expDatasetXml = XML.loadFile((expBag / "metadata" / "dataset.xml").toJava)
        val datasetTransformer = removeElemByName("available")

        // skipping the available field here
        datasetTransformer.transform(datasetXml) should
          equalTrimmed(datasetTransformer.transform(expDatasetXml))
      }

      // in this test we cannot compare the actual contents, since the order of the <file>
      // elements might differ per machine. Therefore we compare all the <file> elements separately.
      it should "check metadata/files.xml" in {
        doNotRunOnTravis()

        val filesXml = XML.loadFile((bag / "metadata" / "files.xml").toJava)
        val expFilesXml = XML.loadFile((expBag / "metadata" / "files.xml").toJava)

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

        val props = paths.outputDepositDir / s"allfields-$bagName" / "deposit.properties"
        val expProps = expectedOutputDir / s"input-$bagName" / "deposit.properties"

        // skipping comment lines, as well as the line with randomized bag-id
        props.lines.toSeq should contain allElementsOf
          expProps.lines
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

  "convert invalidCSV" should "fail in the parser step and return a report of the errors" in {
    val paths = new PathExplorers(
      md = invalidCSV,
      sd = testDir / "sd",
      od = (testDir / "od").createIfNotExists(asDirectory = true, createParents = true))
    val app = new SplitMultiDepositApp(formats, mock[Ldap], DepositPermissions("rwxrwx---", getFileSystemGroup))

    inside(app.convert(paths, "easyadmin")) {
      case Failure(ParserFailedException(report, _)) =>
        report.lines.toSeq should contain inOrderOnly(
          "CSV failures:",
          " - row 2: Only one row is allowed to contain a value for the column 'DEPOSITOR_ID'. Found: [user001, invalid-user]",
          " - row 2: DDM_CREATED value 'invalid-date' does not represent a date",
          " - row 2: Only one row is allowed to contain a value for the column 'DDM_ACCESSRIGHTS'. Found: [OPEN_ACCESS, GROUP_ACCESS]",
          " - row 2: Value 'random test data' is not a valid type",
          " - row 2: Value 'NL' is not a valid value for DC_LANGUAGE",
          " - row 2: DCT_DATE value 'Text with Qualifier' does not represent a date",
          " - row 2: FILE_PATH does not represent a valid path",
          " - row 2: Missing value for: SF_USER",
          " - row 3: DDM_AVAILABLE value 'invalid-date' does not represent a date",
          " - row 3: Missing value for: DC_IDENTIFIER",
          " - row 3: Value 'encoding=UTF-8' is not a valid value for DC_LANGUAGE",
          " - row 3: DCT_DATE_QUALIFIER is only allowed to have a value if DCT_DATE has a well formatted date to go with it",
          " - row 3: FILE_TITLE is not allowed, since FILE_PATH isn't given",
          " - row 4: When DCX_RELATION_LINK is defined, a DCX_RELATION_TITLE must be given as well to provide context",
          " - row 4: DCT_DATE value '30-07-1992' does not represent a date",
          "Due to these errors in the 'instructions.csv', nothing was done."
        )
    }
  }
}
