package nl.knaw.dans.easy.multideposit.actions

import java.io.File

import nl.knaw.dans.easy.multideposit.{Settings, UnitSpec, _}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.util.Success
import scala.collection.JavaConversions.collectionAsScalaIterable

class AddBagToDepositSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    depositDir = new File(testDir, "dd")
  )

  val datasetID = "ds1"
  val file1Text = "abcdef"
  val file2Text = "defghi"
  val file3Text = "ghijkl"
  val file4Text = "jklmno"
  val file5Text = "mnopqr"

  before {
    new File(settings.multidepositDir, s"$datasetID/file1.txt").write(file1Text)
    new File(settings.multidepositDir, s"$datasetID/folder1/file2.txt").write(file2Text)
    new File(settings.multidepositDir, s"$datasetID/folder1/file3.txt").write(file3Text)
    new File(settings.multidepositDir, s"$datasetID/folder2/file4.txt").write(file4Text)
    new File(settings.multidepositDir, s"ds2/folder3/file5.txt").write("file5Text")

    depositBagDir(settings, datasetID).mkdirs
  }

  after {
    depositBagDir(settings, datasetID).deleteDirectory()
  }

  override def afterAll = testDir.getParentFile.deleteDirectory()

  "checkPreconditions" should "fail if the md folder does not exist" in {
    val inputDir = multiDepositDir(settings, datasetID)
    inputDir.deleteDirectory()

    val pre = AddBagToDepositAction(1, datasetID).checkPreconditions

    (the [ActionException] thrownBy pre.get).row shouldBe 1
    (the [ActionException] thrownBy pre.get).message should include (s"does not exist")
  }

  it should "succeed if the md folder exists" in {
    AddBagToDepositAction(1, datasetID).checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "succeed given the current setup" in {
    AddBagToDepositAction(1, datasetID).run() shouldBe a[Success[_]]
  }

  it should "create a bag with the files from ds1 in it and some meta-files around" in {
    AddBagToDepositAction(1, datasetID).run()

    val root = new File(testDir, "dd/md-ds1")
    new File(root, "bag").exists shouldBe true
    FileUtils.listFiles(new File(root, "bag"), null, true)
      .map(_.getName) shouldBe
      List("bag-info.txt",
        "bagit.txt",
        "file1.txt",
        "file2.txt",
        "file3.txt",
        "file4.txt",
        "manifest-md5.txt",
        "tagmanifest-md5.txt")
    new File(root, "bag/data").exists shouldBe true
  }

  it should "preserve the file content after making the bag" in {
    AddBagToDepositAction(1, datasetID).run()

    val root = new File(testDir, "dd/md-ds1")
    new File(root, "bag/data/file1.txt").read shouldBe file1Text
    new File(root, "bag/data/folder1/file2.txt").read shouldBe file2Text
    new File(root, "bag/data/folder1/file3.txt").read shouldBe file3Text
    new File(root, "bag/data/folder2/file4.txt").read shouldBe file4Text
  }

  "rollback" should "always succeed" in {
    AddBagToDepositAction(1, datasetID).rollback() shouldBe a[Success[_]]
  }
}
