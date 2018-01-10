package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Path }

import nl.knaw.dans.easy.multideposit2.PathExplorer.{ InputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit2.TestSupportFixture
import nl.knaw.dans.easy.multideposit2.model.AudioVideo
import nl.knaw.dans.easy.multideposit.FileExtensions
import org.scalatest.BeforeAndAfterEach

import scala.util.Success

class AddPropertiesToDepositSpec extends TestSupportFixture with BeforeAndAfterEach {
  self =>

  private val depositId = "ds1"
  private val datamanagerId = "dm"
  private val action = new AddPropertiesToDeposit with InputPathExplorer with StagingPathExplorer {
    override val multiDepositDir: Path = self.multiDepositDir
    override val stagingDir: Path = self.stagingDir
  }

  override def beforeEach(): Unit = {
    val path = stagingDir.resolve(s"sd-$depositId")
    path.deleteDirectory()
    Files.createDirectories(path)
  }

  "addDepositProperties" should "generate the properties file and write the properties in it" in {
    action.addDepositProperties(testInstructions1.copy(audioVideo = AudioVideo()).toDeposit(), datamanagerId, "dm@test.org") shouldBe a[Success[_]]

    val props = stagingPropertiesFile(testInstructions1.depositId)
    props.toFile should exist

    props.read() should {
      include("creation.timestamp") and
        include("state.label") and
        include("state.description") and
        include(s"depositor.userId=${ testInstructions1.depositorUserId }") and
        include("datamanager.email=dm@test.org") and
        include("datamanager.userId=dm") and
        not include "springfield.domain" and
        not include "springfield.user" and
        not include "springfield.collection" and
        not include "springfield.playmode"
    }
  }

  it should "generate the properties file with springfield fields and write the properties in it" in {
    action.addDepositProperties(testInstructions1.toDeposit(), datamanagerId, "dm@test.org") shouldBe a[Success[_]]

    val props = stagingPropertiesFile(testInstructions1.depositId)
    props.toFile should exist

    props.read() should {
      include("creation.timestamp") and
        include("state.label") and
        include("state.description") and
        include("depositor.userId=ruimtereiziger1") and
        include("datamanager.email=dm@test.org") and
        include("datamanager.userId=dm") and
        include("springfield.domain=dans") and
        include("springfield.user=janvanmansum") and
        include("springfield.collection=Jans-test-files") and
        include("springfield.playmode=menu") and
        include regex "bag-store.bag-id=[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"
    }
  }
}
