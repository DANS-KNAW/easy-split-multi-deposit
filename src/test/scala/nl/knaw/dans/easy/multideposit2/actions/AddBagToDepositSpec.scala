package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Path }
import java.security.MessageDigest

import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms
import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.PathExplorer.{ InputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit2.{ TestSupportFixture, encoding }
import nl.knaw.dans.easy.multideposit2.model.DepositId
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterEach

import scala.util.Success

class AddBagToDepositSpec extends TestSupportFixture with BeforeAndAfterEach { self =>

  private val depositId = "dsId1"
  private val date = new DateTime(1992, 7, 30, 0, 0)
  private val file1Text = "abcdef"
  private val file2Text = "defghi"
  private val file3Text = "ghijkl"
  private val file4Text = "jklmno"
  private val file5Text = "mnopqr"
  private val action = new AddBagToDeposit with StagingPathExplorer with InputPathExplorer {
    val multiDepositDir: Path = self.multiDepositDir
    val stagingDir: Path = self.stagingDir
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    depositDir(depositId).resolve("file1.txt").write(file1Text)
    depositDir(depositId).resolve("folder1/file2.txt").write(file2Text)
    depositDir(depositId).resolve("folder1/file3.txt").write(file3Text)
    depositDir(depositId).resolve("folder2/file4.txt").write(file4Text)
    depositDir("ruimtereis02").resolve("folder3/file5.txt").write(file5Text)

    stagingDir.deleteDirectory()
    Files.createDirectories(stagingBagDir(depositId))
  }

  "addBagToDeposit" should "succeed given the current setup" in {
    action.addBagToDeposit(depositId, date) shouldBe a[Success[_]]
  }

  it should "create a bag with the files from ruimtereis01 in it and some meta-files around" in {
    action.addBagToDeposit(depositId, date) shouldBe a[Success[_]]

    val root = stagingBagDir(depositId)
    root.toFile should exist
    root.listRecursively().map {
      case file if Files.isDirectory(file) => file.getFileName.toString + "/"
      case file => file.getFileName.toString
    } should contain theSameElementsAs
      List("bag/",
        "bag-info.txt",
        "bagit.txt",
        "data/",
        "file1.txt",
        "folder1/",
        "file2.txt",
        "file3.txt",
        "folder2/",
        "file4.txt",
        "manifest-sha1.txt",
        "tagmanifest-sha1.txt")
    stagingBagDataDir(depositId).toFile should exist
  }

  it should "preserve the file content after making the bag" in {
    action.addBagToDeposit(depositId, date) shouldBe a[Success[_]]

    val root = stagingBagDataDir(depositId)
    root.resolve("file1.txt").read() shouldBe file1Text
    root.resolve("folder1/file2.txt").read() shouldBe file2Text
    root.resolve("folder1/file3.txt").read() shouldBe file3Text
    root.resolve("folder2/file4.txt").read() shouldBe file4Text
  }

  it should "create a bag with no files in data when the input directory does not exist" in {
    val action = new AddBagToDeposit with StagingPathExplorer with InputPathExplorer {
      val multiDepositDir: Path = testDir.resolve("md-empty")
      val stagingDir: Path = self.stagingDir
    }

    val bagDir = action.stagingBagDir(depositId)
    Files.createDirectories(bagDir)
    bagDir.toFile should exist

    action.depositDir(depositId).toFile shouldNot exist

    action.addBagToDeposit(depositId, date) shouldBe a[Success[_]]

    action.stagingDir(depositId).toFile should exist
    action.stagingBagDataDir(depositId).toFile should exist
    action.stagingBagDataDir(depositId).listRecursively(!Files.isDirectory(_)) shouldBe empty
    bagDir.listRecursively(!Files.isDirectory(_))
      .map(_.getFileName.toString) should contain theSameElementsAs
      List("bag-info.txt",
        "bagit.txt",
        "manifest-sha1.txt",
        "tagmanifest-sha1.txt")

    bagDir.resolve("manifest-sha1.txt").read() shouldBe empty
    bagDir.resolve("tagmanifest-sha1.txt").read() should include("bag-info.txt")
    bagDir.resolve("tagmanifest-sha1.txt").read() should include("bagit.txt")
    bagDir.resolve("tagmanifest-sha1.txt").read() should include("manifest-sha1.txt")
  }

  it should "contain the date-created in the bag-info.txt" in {
    action.addBagToDeposit(depositId, date) shouldBe a[Success[_]]

    val bagInfo = stagingBagDir(depositId).resolve("bag-info.txt")
    bagInfo.toFile should exist

    bagInfo.read() should include("Created")
  }

  it should "contain the correct checksums in its manifest file" in {
    action.addBagToDeposit(depositId, date) shouldBe a[Success[_]]

    verifyChecksums(depositId, "manifest-sha1.txt")
  }

  it should "contain the correct checksums in its tagmanifest file" in {
    action.addBagToDeposit(depositId, date) shouldBe a[Success[_]]

    verifyChecksums(depositId, "tagmanifest-sha1.txt")
  }

  def verifyChecksums(depositId: DepositId, manifestFile: String): Unit = {
    val root = stagingBagDir(depositId)
    root.resolve(manifestFile).read()
      .split('\n')
      .map(_.split("  "))
      .foreach {
        case Array(sha1, file) => calcSHA1(root.resolve(file).read()) shouldBe sha1
        case line => fail(s"unexpected line detected: ${ line.mkString("  ") }")
      }
  }

  def calcSHA1(string: String): String = {
    MessageDigest.getInstance(StandardSupportedAlgorithms.SHA1.getMessageDigestName)
      .digest(string.getBytes(encoding))
      .map("%02x".format(_))
      .mkString
  }
}