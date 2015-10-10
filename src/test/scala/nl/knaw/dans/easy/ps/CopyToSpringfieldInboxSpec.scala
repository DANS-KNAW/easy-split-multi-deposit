package nl.knaw.dans.easy.ps

import java.io.FileNotFoundException

import nl.knaw.dans.easy.ps.CustomMatchers._
import org.apache.commons.io.FileUtils

import scala.util.Success

class CopyToSpringfieldInboxSpec extends UnitSpec {

  implicit val s = Settings(
    sipDir = file(testDir, "sip"),
    springfieldInbox = file(testDir, "springFieldInbox")
  )

  "checkPreconditions" should "fail if file does not exist" in {

    CopyToSpringfieldInbox(row = "1", fileSip = "videos/some.mpg")
      .checkPreconditions should failWithActionExceptionMatching (row = "1",msg = ".*Cannot find SIP file.*")
  }


  it should "succeed if file exist" in {

    createFile("videos/some.mpg")

    CopyToSpringfieldInbox(row = "1", fileSip = "videos/some.mpg")
      .checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "succeed if file exist" in {

    createFile("videos/some.mpg")

    CopyToSpringfieldInbox(row = "1", fileSip = "videos/some.mpg")
      .run shouldBe a[Success[_]]
  }

  it should "fail if file does not exist" in {

    CopyToSpringfieldInbox(row = "1", fileSip = "videos/some.mpg")
      .run should failWith (a[FileNotFoundException], "videos/some.mpg")
  }

  def createFile(fileName: MdKey): Unit = {
    val theFile = file(s.sipDir, fileName)
    theFile.getParentFile.mkdirs()
    FileUtils.write(theFile, "")
  }
}
