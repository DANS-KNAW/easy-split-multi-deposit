package nl.knaw.dans.easy.ps

import nl.knaw.dans.easy.ps.CustomMatchers._
import org.apache.commons.io.FileUtils._

import scala.util.Success

class AddAmdToDatasetIngestDirSpec extends UnitSpec {
  val distRes = file("src/main/assembly/dist/res")
  implicit val s = Settings(ebiuDir = file(testDir,"ebiuDir"), appHomeDir = file(testDir,"homeDir"))

  "checkPreconditions" should "fail if AMD template does not exist" in {
    s.appHomeDir.mkdirs()
    val result = AddAmdToDatasetIngestDir("1", "dataset-1").checkPreconditions
    result should failWithActionExceptionMatching (row = "1",msg = ".*not found.*")
  }

  it should "fail if AMD schema does not exist" in {
    copyDirectory(distRes,file(s.appHomeDir,"res"))
    file(s.appHomeDir,"res/amd.xsd").delete()
    val result = AddAmdToDatasetIngestDir("1", "dataset-1").checkPreconditions
    result should failWithActionExceptionMatching (row = "1",msg = ".*Failed to read schema document.*")
  }

  it should "fail if AMD template is a directory" in {
    file(s.appHomeDir, "res/administrative-metadata.xml").mkdirs()
    val result = AddAmdToDatasetIngestDir("1", "dataset-1").checkPreconditions
    result should failWithActionExceptionMatching (row = "1",msg = ".*is a directory.*")
  }

  it should "fail if AMD is not a valid AMD file" in {
    copyDirectory(distRes,file(s.appHomeDir,"res"))
    write(file(s.appHomeDir, "res/administrative-metadata.xml"),"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<administrative-md-INVALID />")
    val result = AddAmdToDatasetIngestDir("1", "dataset-1").checkPreconditions
    result should failWithActionExceptionMatching (row = "1",msg = ".*Cannot find the declaration of element.*")
  }

  it should "succeed if AMD is found and valid" in {
    copyDirectory(distRes,file(s.appHomeDir,"res"))
    val result = AddAmdToDatasetIngestDir("1", "dataset-1").checkPreconditions
    result shouldBe a[Success[_]]
  }

  "run" should "succeed and copy AMD if it is found" in {
    copyDirectory(distRes,file(s.appHomeDir,"res"))
    val result = AddAmdToDatasetIngestDir("1", "dataset-1").run()
    result shouldBe a[Success[_]]
    file(s.ebiuDir, "dataset-1", EBIU_METADATA_DIR, EBIU_AMD_FILE) should exist
  }

  it should "fail and copy nothing if AMD is not found" in {
    val result = AddAmdToDatasetIngestDir("1", "dataset-1").run()
    result should failWithActionExceptionMatching (row = "1",msg = ".*administrative-metadata.xml' does not exist")
    file(s.ebiuDir, "dataset-1", EBIU_METADATA_DIR, EBIU_AMD_FILE) shouldNot exist    
  }

  "rollback" should "rely on CreateDatasetIngestDir.rollback to clean up" in {}
}
