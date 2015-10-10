package nl.knaw.dans.easy.ps

import nl.knaw.dans.easy.ps.CustomMatchers._
import org.apache.commons.io.FileUtils.readFileToString

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.Success

class CreateSpringfieldActionsSpec extends UnitSpec {

  implicit val s = Settings(
    sipDir = file(testDir, "sip"),
    springfieldInbox = file(testDir, "springFieldInbox")
  )
  val enough = dataset(Map(
    "SF_DOMAIN" -> List("dans"),
    "SF_USER" -> List("someDeveloper"),
    "SF_COLLECTION" -> List("scala"),
    "SF_PRESENTATION" -> List("unit-test"),
    "FILE_AUDIO_VIDEO" -> List("yes"),
    "FILE_SIP" -> List("videos/some.mpg"),
    "FILE_SUBTITLES" -> List("videos/some.txt"),
    "FILE_DATASET" -> List("footage/some.mpg")
  ))
  def instructions(map: Dataset) = {
    (("dataset-1", map) :: Nil).to[ListBuffer]
  }

  "checkPreconditions" should "succeed allways" in {

    // TODO see PidPackageSpec: property ... forAll ...
    CreateSpringfieldActions(row = "1", instructions(dataset(Map())))
      .checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "create a file when an empty ListBuffer was passed on?" in {

    val instructions = ListBuffer[(DatasetID, mutable.HashMap[MdKey, MdValues])]()
    CreateSpringfieldActions(row = "1", instructions)
      .run shouldBe a[Success[_]]
    file(s.springfieldInbox, "springfield-actions.xml") should be a 'file
  }

  it should "fail with too little instructions" in {

    CreateSpringfieldActions(row = "1", instructions(enough - "FILE_SIP"))
      .run should failWithActionExceptionMatching(row = "1", msg = ".*key not found: FILE_SIP.*")
  }

  it should "create a file with instructions" in {

    CreateSpringfieldActions(row = "1", instructions(enough))
      .run shouldBe a[Success[_]]
    val generated = {
      val xmlFile = file(s.springfieldInbox, "springfield-actions.xml")
      xmlFile should be a 'file
      readFileToString(xmlFile)
    }
    // TODO rather use contains
    generated should fullyMatch regex "(?s).*subtitles=\"videos/some.txt\".*"
    generated should fullyMatch regex "(?s).*src=\"videos/some.mpg\".*"
    generated should fullyMatch regex "(?s).*target=\"/domain/dans/user/someDeveloper/collection/scala/presentation/unit-test\".*"
  }
}
