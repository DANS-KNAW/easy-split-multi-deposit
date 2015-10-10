package nl.knaw.dans.easy.ps

import nl.knaw.dans.easy.ps.CustomMatchers._
import org.apache.commons.io.FileUtils.write

import scala.util.Success

class CopyFileToDatasetIngestDirSpec extends UnitSpec {

  implicit val s = Settings(
    ebiuDir = file(testDir,"ebiuDir"),
    sipDir = file(testDir,"sip001")
  )
  file(s.sipDir,"some/sip/dir/subdir").mkdirs()
  write(file(s.sipDir,"some/sip/dir/subdir/file-in-subdir.txt")
    ,"")
  write(file(s.sipDir,"some/sip/dir/file-in-dir.txt")
    ,"")
  write(file(s.sipDir,"some/sip/file.txt")
    ,"some content")

  "checkPreconditions" should "fail if FILE_SIP does not exist" in {
    CopyFileToDatasetIngestDir("1", "datasetId", fileSip = "non-existent FILE_SIP", "fileInDataset")
      .checkPreconditions should failWithActionExceptionMatching (row = "1",msg = ".*does not exist.*")
  }

  it should "succeed if FILE_SIP exists and is a file" in {
    CopyFileToDatasetIngestDir("1", "dataset-1", fileSip = "some/sip/file.txt", "fileInDataset")
      .checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed if FILE_SIP exists and is a directory" in {
    CopyFileToDatasetIngestDir("1", "dataset-1", fileSip = "some/sip/dir", "fileInDataset")
      .checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "copy file if FILE_SIP is single file" in {
    CopyFileToDatasetIngestDir("1",
      datasetId = "dataset-1",
      fileSip = "some/sip/file.txt",
      fileDataset = "some/dir/file-in-dataset.txt"
    ).run shouldBe a[Success[_]]
    val targetFile = file(s.ebiuDir, "dataset-1", EBIU_FILEDATA_DIR, "some/dir/file-in-dataset.txt")
    targetFile.isFile should be(true)
  }

  it should "copy directory tree if FILE_SIP is directory" in {
    CopyFileToDatasetIngestDir("1",
      datasetId = "dataset-1",
      fileSip = "some/sip/dir",
      fileDataset = "some/dir-in-dataset"
    ).run shouldBe a[Success[_]]
    val targetDir = file(s.ebiuDir, "dataset-1", EBIU_FILEDATA_DIR, "some/dir-in-dataset")
    file(targetDir, "file-in-dir.txt") should be a 'file
    file(targetDir, "subdir/file-in-subdir.txt") should be a 'file
  }

  "rollback" should "rely on CreateDatasetIngetDir.rollback to cleanup" in {}
}
