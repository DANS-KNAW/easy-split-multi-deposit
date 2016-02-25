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

import java.io.{File, FileNotFoundException}

import nl.knaw.dans.easy.ps.CustomMatchers._
import org.apache.commons.io.FileUtils
import org.scalatest.Ignore
import org.xml.sax.SAXParseException

import scala.util.Success
import scala.xml.{Node, PrettyPrinter}

@Ignore
class ValidateXmlSpec extends UnitSpec {

  def testFile(fileName: String, content: Node): File = {
    val newFile = file(testDir, fileName)
    FileUtils.write(newFile, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
      new PrettyPrinter(160, 2).format(content).toString
    )
    newFile
  }

  val xsdFile = testFile("test.xsd",
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
               targetNamespace="http://testnamespace">
      <xs:element name="test-element"/>
    </xs:schema>)

  "validateXml" should "fail if XML file does not exist" in {

    validateXml(file(testDir,"non-existent.xml"), xsdFile) should
      failWith(a[FileNotFoundException], "not found")
  }

  it should "fail if XML file is a folder" in {

    validateXml(testDir, xsdFile) should
      failWith(a[FileNotFoundException], "is a directory")
  }

  it should "fail if XML file contains anything but xml" in {

    FileUtils.write(file(testDir, "rubish.xml"), "rababera\ntralala")
    validateXml(file(testDir, "rubish.xml"), xsdFile) should
      failWith(a[SAXParseException], "Content is not allowed in prolog")
  }

  it should "fail if XML does not conform to XSD" in {

    validateXml(testFile("invalid.xml",
        <test-element-INVALID xmlns="http://testnamespace" />
    ), xsdFile) should
      failWith(a[SAXParseException], "Cannot find the declaration of element")
  }

  it should "succeed if XML conforms to XSD" in {

    validateXml(testFile("valid.xml",
        <test-element xmlns="http://testnamespace" />
    ), xsdFile) shouldBe a[Success[_]]
  }
}
