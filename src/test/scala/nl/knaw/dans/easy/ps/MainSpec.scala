/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.ps

import nl.knaw.dans.easy.ps.CustomMatchers._

import org.apache.commons.io.FileUtils._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import java.util

import scala.collection.mutable.ListBuffer
import org.apache.commons.io.FileUtils._
import collection.JavaConversions._

class MainSpec extends UnitSpec {

  private val minimalInstructionsHeader =
    "DATASET_ID,DC_TITLE,DC_DESCRIPTION,DCX_CREATOR_INITIALS,DCX_CREATOR_SURNAME,DDM_CREATED,DDM_AUDIENCE,DDM_ACCESSRIGHTS," +
    "SF_DOMAIN,SF_USER,SF_COLLECTION,SF_PRESENTATION,FILE_AUDIO_VIDEO,FILE_SIP,FILE_DATASET"

  /** work around the main method that logs messages we want to examine */
  private def mockMain(implicit settings: Settings): Try[Unit] = {

    var result: Try[Unit] = null
    Main.getActionsStream.subscribe(
      onNext = x => (),
      onError = e => result=Failure(e),
      onCompleted = () => result=Success(Unit)
    )
    result
  }

  "main" should "succeed with Roundtrip/sip-demo-2015-02-24" in {

    implicit val s = mockSettings()

    s.ebiuDir.mkdirs()
    copyDirectory(file("src/test/resources/Roundtrip/sip-demo-2015-02-24"), s.sipDir)
    copyDirectory(file("src/main/assembly/dist/res"), file(s.appHomeDir,"res"))

    mockMain shouldBe a[Success[_]]
    file(s.ebiuDir,"ruimtereis01/filedata/footage/centaur.mpg.properties") should be a 'file
    file(s.ebiuDir,"ruimtereis01/filedata/reisverslag/deel01.txt") should be a 'file
    file(s.ebiuDir,"ruimtereis01/filedata/reisverslag/deel02.txt") should be a 'file
    file(s.ebiuDir,"ruimtereis01/filedata/reisverslag/deel03.txt") should be a 'file
    file(s.ebiuDir,"ruimtereis01/metadata/administrative-metadata.xml") should be a 'file
    file(s.ebiuDir,"ruimtereis01/metadata/easymetadata.xml") should be a 'file
    file(s.ebiuDir,"ruimtereis02/filedata/Hubble-beschrijving.txt") should be a 'file
    file(s.ebiuDir,"ruimtereis02/filedata/Hubble-description.txt") should be a 'file
    file(s.ebiuDir,"ruimtereis02/filedata/img/hubble-big.jpg.properties") should be a 'file
    file(s.ebiuDir,"ruimtereis02/filedata/img/hubble-collage.jpg") should be a 'file
    file(s.ebiuDir,"ruimtereis02/filedata/video/hubble.mpg.properties") should be a 'file
    file(s.ebiuDir,"ruimtereis02/metadata/administrative-metadata.xml") should be a 'file
    file(s.ebiuDir,"ruimtereis02/metadata/easymetadata.xml") should be a 'file
    file(s.springfieldInbox,"videos/centaur.mpg") should be a 'file
    file(s.springfieldInbox,"videos/hubble.mpg") should be a 'file
    file(s.springfieldInbox,"springfield-actions.xml") should be a 'file
    file(storageLocation,"zandbak/special/fotos/hubble-big.jpg") should be a 'file
    file(storageLocation,"zandbak/special/videos/hubble-video.mpg") should be a 'file
    file(storageLocation,"zandbak11.dans.knaw.nl/webdav/no-organization/Kirk/Reis-naar-Centaur-planetoide/footage/centaur.mpg") should be a 'file
  }

  it should "complain about invalid combinations with Roundtrip/sip001" in {

    implicit val s = mockSettings()
    s.ebiuDir.mkdirs()
    copyDirectory(file("src/test/resources/Roundtrip/sip001"), s.sipDir)

    mockMain should failWith (a[Exception],"Errors in SIP Instructions","Invalid combination","row 4","row 7","row 9")
  }

  it should "neither complain nor copy anything with just a header in the instructions file" in {

    implicit val s = mockSettings()
    copyDirectory(file("src/main/assembly/dist/res"), file(s.appHomeDir,"res"))
    s.ebiuDir.mkdirs()
    write(file(s.sipDir, "instructions.csv"), minimalInstructionsHeader)
    write(file(s.sipDir, "someFolder/someFile.txt"), "some content")
    write(file(s.sipDir, "someFile.txt"), "some content")

    mockMain shouldBe a[Success[_]]
    file(s.ebiuDir).list() should have length 0
    readFileToString(file(s.springfieldInbox, "springfield-actions.xml")) should be("<actions></actions>")
    file(s.springfieldInbox).list().length shouldBe 1
  }

  it should "fail with empty instructions file" in {

    implicit val s = mockSettings()
    copyDirectory(file("src/main/assembly/dist/res"), file(s.appHomeDir,"res"))
    s.ebiuDir.mkdirs()
    write(file(s.sipDir, "instructions.csv"), "")

    the[NoSuchElementException] thrownBy mockMain should have message "next on empty iterator"
  }

  it should "prepare a dataset when given only EMD data in the instructions file" in {

    implicit val s = mockSettings()
    copyDirectory(file("src/main/assembly/dist/res"), file(s.appHomeDir,"res"))
    s.ebiuDir.mkdirs()
    write(file(s.sipDir, "dataset1/someFile.txt"), "some content")
    write(file(s.sipDir, "dataset1/someFolder/someFileInFolder.txt"), "some content")
    write(file(s.sipDir, "instructions.csv"), minimalInstructionsHeader +
      "\ndataset1,titel,beschrijving,A.,Developer,2014,D32000,OPEN_ACCESS,,,,,,,"
    )

    mockMain shouldBe a[Success[_]]
    readFileToString(file(s.springfieldInbox, "springfield-actions.xml")) should be("<actions></actions>")
    file(s.ebiuDir,"dataset1/filedata/someFile.txt") should be a 'file
    file(s.ebiuDir,"dataset1/filedata/someFolder/someFileInFolder.txt") should be a 'file
    file(s.ebiuDir,"dataset1/metadata/easymetadata.xml") should be a 'file
    file(s.ebiuDir,"dataset1/metadata/administrative-metadata.xml") should be a 'file
  }
  // TODO a dynamic ignore like for AddEmdToDatasetIngestDirSpec
}
