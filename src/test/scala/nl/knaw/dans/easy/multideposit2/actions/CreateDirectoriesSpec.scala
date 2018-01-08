package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Path }

import nl.knaw.dans.easy.multideposit2.PathExplorer.{ InputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit2.TestSupportFixture
import org.scalatest.BeforeAndAfterEach
import nl.knaw.dans.easy.multideposit.FileExtensions

import scala.util.{ Failure, Success }

class CreateDirectoriesSpec extends TestSupportFixture with BeforeAndAfterEach {
  self =>

  private val depositId = "dsId1"
  private val action = new CreateDirectories with StagingPathExplorer with InputPathExplorer {
    override val multiDepositDir: Path = self.multiDepositDir
    override val stagingDir: Path = self.stagingDir
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    // create depositDir base directory
    stagingDir.deleteDirectory()
    Files.createDirectory(stagingDir)
    stagingDir.toFile should exist
  }

  "createDepositDirectories" should "create the staging directories if they do not yet exist" in {
    stagingDir(depositId).toFile shouldNot exist
    stagingBagDir(depositId).toFile shouldNot exist

    action.createDepositDirectories(depositId) shouldBe a[Success[_]]

    stagingDir(depositId).toFile should exist
    stagingBagDir(depositId).toFile should exist
  }

  it should "fail if any of the directories already exist" in {
    Files.createDirectories(stagingBagDir(depositId))

    inside(action.createDepositDirectories(depositId)) {
      case Failure(ActionException(msg, _)) => msg should include(s"The deposit $depositId already exists")
    }
  }
}
