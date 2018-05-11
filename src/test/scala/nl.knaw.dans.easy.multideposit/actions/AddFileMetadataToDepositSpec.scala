/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit.actions

import better.files.File
import nl.knaw.dans.easy.multideposit.model.{ AVFileMetadata, DefaultFileMetadata, FileAccessRights, Subtitles, Video }
import nl.knaw.dans.easy.multideposit.{ CustomMatchers, TestSupportFixture }
import org.scalatest.BeforeAndAfterEach

import scala.util.Success
import scala.xml.XML

class AddFileMetadataToDepositSpec extends TestSupportFixture with CustomMatchers with BeforeAndAfterEach {

  private val action = new AddFileMetadataToDeposit
  private val depositId = "ruimtereis01"

  override def beforeEach(): Unit = {
    if (multiDepositDir.exists) multiDepositDir.delete()
    multiDepositDir.createDirectory()
    multiDepositDir.toJava should exist

    val mimetypesDir = testDir / "mimetypes"
    if (mimetypesDir.exists) mimetypesDir.delete()
    mimetypesDir.createDirectory()
    mimetypesDir.toJava should exist

    File(getClass.getResource("/allfields/input").toURI).copyTo(multiDepositDir)
    File(getClass.getResource("/mimetypes").toURI).copyTo(mimetypesDir)
  }

  "execute" should "write the file metadata to an xml file" in {
    val fileMetadata = Seq(
      AVFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg",
        mimeType = "video/mpeg",
        vocabulary = Video,
        title = "centaur.mpg",
        accessibleTo = FileAccessRights.NONE
      )
    )

    action.addFileMetadata(depositId, fileMetadata) shouldBe a[Success[_]]
    stagingBagMetadataDir(depositId).toJava should exist
    stagingFileMetadataFile(depositId).toJava should exist
  }

  it should "produce the xml for all the files" in {
    val fileMetadata = Seq(
      AVFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg",
        mimeType = "video/mpeg",
        vocabulary = Video,
        title = "video about the centaur meteorite",
        accessibleTo = FileAccessRights.RESTRICTED_GROUP,
        subtitles = Set(
          Subtitles(multiDepositDir / "ruimtereis01/reisverslag/centaur.srt", Option("en")),
          Subtitles(multiDepositDir / "ruimtereis01/reisverslag/centaur-nederlands.srt", Option("nl"))
        )
      ),
      AVFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/path/to/a/random/video/hubble.mpg",
        mimeType = "video/mpeg",
        vocabulary = Video,
        title = "hubble.mpg",
        accessibleTo = FileAccessRights.RESTRICTED_GROUP
      )
    )

    action.addFileMetadata(depositId, fileMetadata) shouldBe a[Success[_]]

    val actual = XML.loadFile(stagingFileMetadataFile(depositId).toJava)
    val expected = XML.loadFile(File(getClass.getResource("/allfields/output/input-ruimtereis01/bag/metadata/files.xml").toURI).toJava)

    actual \ "files" should containAllNodes(expected \ "files")
  }

  it should "produce the xml for a deposit with no A/V files" in {
    val depositId = "ruimtereis02"
    val fileMetadata = Seq(
      DefaultFileMetadata(
        filepath = testDir / "md/ruimtereis02/path/to/images/Hubble_01.jpg",
        mimeType = "image/jpg",
        title = Some("Hubble"),
        accessibleTo = Some(FileAccessRights.RESTRICTED_REQUEST)
      )
    )
    action.addFileMetadata(depositId, fileMetadata) shouldBe a[Success[_]]

    val actual = XML.loadFile(stagingFileMetadataFile(depositId).toJava)
    val expected = XML.loadFile(File(getClass.getResource("/allfields/output/input-ruimtereis02/bag/metadata/files.xml").toURI).toJava)

    actual \ "files" should containAllNodes(expected \ "files")
  }

  it should "produce the xml for a deposit with no files" in {
    val depositId = "ruimtereis03"
    action.addFileMetadata(depositId, Seq.empty) shouldBe a[Success[_]]

    val actual = XML.loadFile(stagingFileMetadataFile(depositId).toJava)
    val expected = XML.loadFile(File(getClass.getResource("/allfields/output/input-ruimtereis03/bag/metadata/files.xml").toURI).toJava)

    actual should equalTrimmed(expected)
  }
}
