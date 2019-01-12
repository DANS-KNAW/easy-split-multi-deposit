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
package nl.knaw.dans.easy.multideposit.parser

import better.files.File
import cats.data.Validated.{ Invalid, Valid }
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import org.scalatest.BeforeAndAfterEach

class MimeTypeSpec extends TestSupportFixture with BeforeAndAfterEach {

  private val mimetypesDir = testDir / "mimetypes"

  override def beforeEach(): Unit = {
    super.beforeEach()

    if (mimetypesDir.exists) mimetypesDir.delete()
    File(getClass.getResource("/mimetypes").toURI).copyTo(mimetypesDir)
  }

  "getMimeType" should "produce the correct doc mimetype" in {
    MimeType.get(mimetypesDir / "file-ms-doc.doc") shouldBe Valid("application/msword")
  }

  it should "produce the correct docx mimetype" in {
    MimeType.get(mimetypesDir / "file-ms-docx.docx") shouldBe Valid("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
  }

  it should "produce the correct xlsx mimetype" in {
    MimeType.get(mimetypesDir / "file-ms-excel.xlsx") shouldBe Valid("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
  }

  it should "produce the correct pdf mimetype" in {
    MimeType.get(mimetypesDir / "file-pdf.pdf") shouldBe Valid("application/pdf")
  }

  it should "produce the correct plain text mimetype" in {
    MimeType.get(mimetypesDir / "file-plain-text.txt") shouldBe Valid("text/plain")
  }

  it should "produce the correct json mimetype" in {
    MimeType.get(mimetypesDir / "file-json.json") shouldBe Valid("application/json")
  }

  it should "produce the correct xml mimetype" in {
    MimeType.get(mimetypesDir / "file-xml.xml") shouldBe Valid("application/xml")
  }

  it should "give the correct mimetype if the file is plain text and has no extension" in {
    MimeType.get(mimetypesDir / "file-unknown") shouldBe Valid("text/plain")
  }

  it should "give the correct mimetype if the file has no extension" in {
    MimeType.get(mimetypesDir / "file-unknown-pdf") shouldBe Valid("application/pdf")
  }

  it should "give the correct mimetype if the file is hidden" in {
    MimeType.get(mimetypesDir / ".file-hidden-pdf") shouldBe Valid("application/pdf")
  }

  it should "fail if the file does not exist" in {
    inside(MimeType.get(mimetypesDir / "file-does-not-exist.doc").leftMap(_.toNonEmptyList.toList)) {
      case Invalid(List(ParseError(-1, message))) =>
        message should include("mimetypes/file-does-not-exist.doc")
    }
  }
}
