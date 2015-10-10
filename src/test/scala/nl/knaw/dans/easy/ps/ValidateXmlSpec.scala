package nl.knaw.dans.easy.ps

import java.io.{File, FileNotFoundException}

import nl.knaw.dans.easy.ps.CustomMatchers._
import org.apache.commons.io.FileUtils
import org.xml.sax.SAXParseException

import scala.util.Success
import scala.xml.{Node, PrettyPrinter}

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
