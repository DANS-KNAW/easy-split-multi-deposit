package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Paths }

import nl.knaw.dans.easy.multideposit.{ CustomMatchers, FileExtensions }
import nl.knaw.dans.easy.multideposit2.TestSupportFixture
import nl.knaw.dans.easy.multideposit2.model.{ AVFileMetadata, DefaultFileMetadata, FileAccessRights, Subtitles, Video }
import org.scalatest.BeforeAndAfterEach

import scala.util.Success
import scala.xml.XML

class AddFileMetadataToDepositSpec extends TestSupportFixture with CustomMatchers with BeforeAndAfterEach {

  private val action = new AddFileMetadataToDeposit
  private val depositId = "ruimtereis01"

  override def beforeEach(): Unit = {
    multiDepositDir.deleteDirectory()
    Files.createDirectory(multiDepositDir)
    multiDepositDir.toFile should exist

    Paths.get(getClass.getResource("/allfields/input").toURI).copyDir(multiDepositDir)
    Paths.get(getClass.getResource("/mimetypes").toURI).copyDir(testDir.resolve("mimetypes"))
  }

  "execute" should "write the file metadata to an xml file" in {
    val fileMetadata = Seq(
      AVFileMetadata(
        filepath = Paths.get("ruimtereis01/reisverslag/centaur.mpg").toAbsolutePath,
        mimeType = "video/mpeg",
        vocabulary = Video,
        title = "centaur.mpg",
        accessibleTo = FileAccessRights.NONE
      )
    )

    action.addFileMetadata(depositId, fileMetadata) shouldBe a[Success[_]]
    stagingBagMetadataDir(depositId).toFile should exist
    stagingFileMetadataFile(depositId).toFile should exist
  }

  it should "produce the xml for all the files" in {
    val fileMetadata = Seq(
      AVFileMetadata(
        filepath = multiDepositDir.resolve("ruimtereis01/reisverslag/centaur.mpg"),
        mimeType = "video/mpeg",
        vocabulary = Video,
        title = "video about the centaur meteorite",
        accessibleTo = FileAccessRights.RESTRICTED_GROUP,
        subtitles = Set(
          Subtitles(multiDepositDir.resolve("ruimtereis01/reisverslag/centaur.srt").toAbsolutePath, Option("en")),
          Subtitles(multiDepositDir.resolve("ruimtereis01/reisverslag/centaur-nederlands.srt").toAbsolutePath, Option("nl"))
        )
      ),
      AVFileMetadata(
        filepath = multiDepositDir.resolve("ruimtereis01/path/to/a/random/video/hubble.mpg"),
        mimeType = "video/mpeg",
        vocabulary = Video,
        title = "hubble.mpg",
        accessibleTo = FileAccessRights.RESTRICTED_GROUP
      )
    )

    action.addFileMetadata(depositId, fileMetadata) shouldBe a[Success[_]]

    val actual = XML.loadFile(stagingFileMetadataFile(depositId).toFile)
    val expected = XML.loadFile(Paths.get(getClass.getResource("/allfields/output/input-ruimtereis01/bag/metadata/files.xml").toURI).toFile)

    actual \ "files" should containAllNodes(expected \ "files")
  }

  it should "produce the xml for a deposit with no A/V files" in {
    val depositId = "ruimtereis02"
    val fileMetadata = Seq(
      DefaultFileMetadata(
        filepath = testDir.resolve("md/ruimtereis02/path/to/images/Hubble_01.jpg"),
        mimeType = "image/jpg",
        title = Some("Hubble"),
        accessibleTo = Some(FileAccessRights.RESTRICTED_REQUEST)
      )
    )
    action.addFileMetadata(depositId, fileMetadata) shouldBe a[Success[_]]

    val actual = XML.loadFile(stagingFileMetadataFile(depositId).toFile)
    val expected = XML.loadFile(Paths.get(getClass.getResource("/allfields/output/input-ruimtereis02/bag/metadata/files.xml").toURI).toFile)

    actual \ "files" should containAllNodes(expected \ "files")
  }

  it should "produce the xml for a deposit with no files" in {
    val depositId = "ruimtereis03"
    action.addFileMetadata(depositId, Seq.empty) shouldBe a[Success[_]]

    val actual = XML.loadFile(stagingFileMetadataFile(depositId).toFile)
    val expected = XML.loadFile(Paths.get(getClass.getResource("/allfields/output/input-ruimtereis03/bag/metadata/files.xml").toURI).toFile)

    actual should equalTrimmed(expected)
  }
}