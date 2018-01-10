package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Path, Paths }

import nl.knaw.dans.easy.multideposit2.PathExplorer.PathExplorers
import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.TestSupportFixture
import org.scalatest.BeforeAndAfterEach

import scala.util.Success

class MoveDepositToOutputDirSpec extends TestSupportFixture with BeforeAndAfterEach {
  self =>

  private val depositId = "ruimtereis01"
  private val action: MoveDepositToOutputDir = new MoveDepositToOutputDir with PathExplorers {
    override val multiDepositDir: Path = self.multiDepositDir
    override val stagingDir: Path = self.stagingDir
    override val outputDepositDir: Path = self.outputDepositDir
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    // create stagingDir content
    stagingDir.deleteDirectory()
    Files.createDirectory(stagingDir)
    stagingDir.toFile should exist

    Paths.get(getClass.getResource("/allfields/output/input-ruimtereis01").toURI)
      .copyDir(stagingDir("ruimtereis01"))
    Paths.get(getClass.getResource("/allfields/output/input-ruimtereis02").toURI)
      .copyDir(stagingDir("ruimtereis02"))

    stagingDir("ruimtereis01").toFile should exist
    stagingDir("ruimtereis02").toFile should exist

    outputDepositDir.deleteDirectory()
    Files.createDirectory(outputDepositDir)
    outputDepositDir.toFile should exist
  }

  "execute" should "move the deposit to the outputDepositDirectory" in {
    action.moveDepositsToOutputDir(depositId) shouldBe a[Success[_]]

    stagingDir(depositId).toFile shouldNot exist
    outputDepositDir(depositId).toFile should exist

    stagingDir("ruimtereis02").toFile should exist
    outputDepositDir("ruimtereis02").toFile shouldNot exist
  }

  it should "only move the one deposit to the outputDepositDirectory, not other deposits in the staging directory" in {
    action.moveDepositsToOutputDir(depositId) shouldBe a[Success[_]]

    stagingDir("ruimtereis02").toFile should exist
    outputDepositDir("ruimtereis02").toFile shouldNot exist
  }
}
