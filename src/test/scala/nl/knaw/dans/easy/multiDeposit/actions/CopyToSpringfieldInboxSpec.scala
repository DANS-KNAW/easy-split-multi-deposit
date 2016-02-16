package nl.knaw.dans.easy.multiDeposit.actions

import java.io.{FileNotFoundException, File}

import nl.knaw.dans.easy.multiDeposit._
import nl.knaw.dans.easy.multiDeposit.{ActionException, Settings, UnitSpec}
import nl.knaw.dans.easy.ps.MdKey

import scala.util.Success

class CopyToSpringfieldInboxSpec extends UnitSpec {

  implicit val settings = Settings(
    mdDir = new File(testDir, "md"),
    springfieldInbox = new File(testDir, "springFieldInbox")
  )

  def createFile(fileName: MdKey) = {
    val file = new File(settings.mdDir, fileName)
    file.getParentFile.mkdirs
    file.write("")
  }

  "checkPreconditions" should "fail if file does not exist" in {
    val pre = CopyToSpringfieldInbox(1, "videos/some_checkPreFail.mpg").checkPreconditions

    (the [ActionException] thrownBy pre.get).row shouldBe 1
    (the [ActionException] thrownBy pre.get).message should include ("Cannot find MD file:")
  }

  it should "succeed if file exist" in {
    createFile("videos/some_checkPreSuccess.mpg")

    CopyToSpringfieldInbox(1, "videos/some_checkPreSuccess.mpg").checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "succeed if file exist" in {
    createFile("videos/some.mpg")

    CopyToSpringfieldInbox(1, "videos/some.mpg").run shouldBe a[Success[_]]
  }

  it should "fail if file does not exist" in {
    val run = CopyToSpringfieldInbox(1, "videos/some_error.mpg").run
    (the [FileNotFoundException] thrownBy run.get).getMessage should include ("videos/some_error.mpg")
  }

  "rollback" should "always succeed" in {
    CopyToSpringfieldInbox(1, "videos/some_rollback.mpg").rollback shouldBe a[Success[_]]
  }
}
