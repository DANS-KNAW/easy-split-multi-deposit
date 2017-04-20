package nl.knaw.dans.easy.multideposit

import java.io.File
import javax.naming.directory.{ Attributes, BasicAttribute, BasicAttributes }

import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter

import scala.util.{ Failure, Success }

class IntegrationSpec extends UnitSpec with BeforeAndAfter with MockFactory {

  private val allfields = new File(testDir, "md/allfields")
  private val invalidCSV = new File(testDir, "md/invalidCSV")

  before {
    new File(getClass.getResource("/allfields/input").toURI).copyDir(allfields)
    new File(getClass.getResource("/invalidCSV/input").toURI).copyDir(invalidCSV)
  }

  "allfields" should "succeed in transforming the input into a bag" in {
    val ldap = mock[Ldap]
    implicit val settings = Settings(
      multidepositDir = allfields,
      stagingDir = new File(testDir, "sd"),
      outputDepositDir = new File(testDir, "od"),
      datamanager = "me",
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
        put("mail", "dm@test.org")
      }
    }

    (ldap.query(_: String)(_: Attributes => Attributes)) expects (settings.datamanager, *) returning Success(Seq(createDatamanagerAttributes))
    (ldap.query(_: String)(_: Attributes => Boolean)) expects ("user001", *) repeat 3 returning Success(Seq(true))

    Main.run shouldBe a[Success[_]]

    for (bagName <- Seq("ruimtereis01", "ruimtereis02", "ruimtereis03")) {
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
      val expDatasetXml = new File(bag, "metadata/dataset.xml")
      datasetXml.read().lines.toSeq should contain allElementsOf expDatasetXml.read().lines.toSeq

      val filesXml = new File(bag, "metadata/files.xml")
      val expFilesXml = new File(bag, "metadata/files.xml")
      filesXml.read().lines.toSeq should contain allElementsOf expFilesXml.read().lines.toSeq
    }
  }

  "invalidCSV" should "fail in the parser step and return a report of the errors" in {
    implicit val settings = Settings(
      multidepositDir = invalidCSV
    )

    inside(Main.run) {
      case Failure(ParserFailedException(report, _)) =>
        report should {
          include ("- row 2: There are multiple distinct depositorIDs in dataset 'ruimtereis01': [user001, invalid-user]") and
          include ("- row 2: DDM_CREATED value 'invalid-date' does not represent a date") and
          include ("- row 3: DDM_AVAILABLE value 'invalid-date' does not represent a date") and
          include ("- row 2: Only one row is allowed to contain a value for the column: 'DDM_ACCESSRIGHTS'") and
          include ("- row 2: Missing value for: SF_USER") and
          include (s"- row 2: AV_FILE file '${settings.multidepositDir.getAbsolutePath}/ruimtereis01/path/to/audiofile/that/does/not/exist.mp3' does not exist") and
          include ("- row 3: No value is defined for AV_FILE, while some of [AV_FILE_TITLE, AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined") and
          include ("Due to these errors in the 'instructions.csv', nothing was done.")
        }
    }
  }
}
