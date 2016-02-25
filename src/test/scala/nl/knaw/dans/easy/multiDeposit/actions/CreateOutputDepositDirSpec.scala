package nl.knaw.dans.easy.multiDeposit.actions

import java.io.File

import nl.knaw.dans.easy.multiDeposit.{Settings, UnitSpec, _}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.util.Success

class CreateOutputDepositDirSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    depositDir = new File(testDir, "dd")
  )

  override def beforeAll = {
    testDir.mkdir
    testDir.exists shouldBe true
  }

  before {
    // create depositDir base directory
    val baseDir = settings.depositDir
    baseDir.mkdir
    baseDir.exists shouldBe true
  }

  after {
    // clean up stuff after the test is done
    val baseDir = settings.depositDir
    baseDir.deleteDirectory()
    baseDir.exists shouldBe false
  }

  override def afterAll = {
    testDir.deleteDirectory()
    testDir.exists shouldBe false
  }

  "checkPreconditions" should "always succeed" in {
    CreateOutputDepositDir(1, "ds1").checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "create the directories" in {
    // test is in seperate function,
    // since we want to reuse the code
    runTest()
  }

  "rollback" should "" in {
    // setup for this test
    runTest()

    // roll back the creation of the directories
    CreateOutputDepositDir(1, "ds1").rollback() shouldBe a[Success[_]]

    // test that the directories are really not there anymore
    new File(testDir, "dd/md-ds1").exists shouldBe false
    new File(testDir, "dd/md-ds1/bag").exists shouldBe false
    new File(testDir, "dd/md-ds1/bag/metadata").exists shouldBe false
  }

  def runTest(): Unit = {
    // directories do not exist before
    new File(testDir, "dd/md-ds1").exists shouldBe false
    new File(testDir, "dd/md-ds1/bag").exists shouldBe false
    new File(testDir, "dd/md-ds1/bag/metadata").exists shouldBe false

    // creation of directories
    CreateOutputDepositDir(1, "ds1").run() shouldBe a[Success[_]]

    // test existance after creation
    new File(testDir, "dd/md-ds1").exists shouldBe true
    new File(testDir, "dd/md-ds1/bag").exists shouldBe true
    new File(testDir, "dd/md-ds1/bag/metadata").exists shouldBe true
  }

}
