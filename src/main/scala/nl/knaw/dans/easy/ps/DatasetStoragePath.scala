package nl.knaw.dans.easy.ps

import org.apache.commons.lang.StringUtils.isBlank

case class DatasetStoragePath(dataset: Dataset) {
  private val path: Option[String] = calculatedDatasetStoragePath
  
  private def calculatedDatasetStoragePath: Option[String] = {
    for {
      
      // TODO: handle cases where some of these columns do not exist
      a <- getFirstNonBlank(dataset("DCX_ORGANIZATION") ::: List("no-organization"))
      b <- getFirstNonBlank(dataset("DCX_CREATOR_DAI") ::: dataset("DCX_CREATOR_SURNAME") ::: dataset("DC_CREATOR"))
      c <- getFirstNonBlank(dataset("DC_TITLE"))
    } yield List(a, b, c).mkString("/")
  }

  private def getFirstNonBlank(list: List[String]): Option[String] =
    list.dropWhile(isBlank).headOption
  
  def get: Option[String] = path
}