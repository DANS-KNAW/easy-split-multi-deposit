package nl.knaw.dans.easy.multideposit2.actions

import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.PathExplorer.{ InputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit2.model._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.control.NonFatal
import scala.util.{ Failure, Try }
import scala.xml.{ Elem, NodeSeq }

trait AddFileMetadataToDeposit extends DebugEnhancedLogging {
  this: InputPathExplorer with StagingPathExplorer =>

  def addFileMetadata(depositId: DepositId, fileMetadata: Seq[FileMetadata]): Try[Unit] = Try {
    logger.debug(s"add file metadata for $depositId")

    stagingFileMetadataFile(depositId).writeXml(depositToFileXml(depositId, fileMetadata))
  } recoverWith {
    case NonFatal(e) => Failure(ActionException(s"Could not write file metadata", e))
  }

  private def depositToFileXml(depositId: DepositId, fileMetadata: Seq[FileMetadata]): Elem = {
    fileXmls(depositId, fileMetadata) match {
      case Nil => <files
        xmlns:dcterms="http://purl.org/dc/terms/"
        xmlns="http://easy.dans.knaw.nl/schemas/bag/metadata/files/"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation={"http://purl.org/dc/terms/ http://dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd " +
          "http://easy.dans.knaw.nl/schemas/bag/metadata/files/ http://easy.dans.knaw.nl/schemas/bag/metadata/files/files.xsd"}/>
      case files => <files
        xmlns:dcterms="http://purl.org/dc/terms/"
        xmlns="http://easy.dans.knaw.nl/schemas/bag/metadata/files/"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation={"http://purl.org/dc/terms/ http://dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd " +
          "http://easy.dans.knaw.nl/schemas/bag/metadata/files/ http://easy.dans.knaw.nl/schemas/bag/metadata/files/files.xsd"}>{files}</files>
    }
  }

  private def fileXmls(depositId: DepositId, fmds: Seq[FileMetadata]): Seq[Elem] = {
    fmds.map {
      case av: AVFileMetadata => avFileXml(depositId, av)
      case fmd: DefaultFileMetadata => defaultFileXml(depositId, fmd)
    }
  }

  private def defaultFileXml(depositId: DepositId, fmd: DefaultFileMetadata): Elem = {
    <file filepath={s"data/${ depositDir(depositId).relativize(fmd.filepath) }"}>
      <dcterms:format>{fmd.mimeType}</dcterms:format>
      {fmd.title.map(title => <dcterms:title>{title}</dcterms:title>).getOrElse(NodeSeq.Empty)}
      {fmd.accessibleTo.map(act => <accessibleToRights>{act}</accessibleToRights>).getOrElse(NodeSeq.Empty)}
    </file>
  }

  private def avFileXml(depositId: DepositId, fmd: AVFileMetadata): Elem = {
    <file filepath={s"data/${ depositDir(depositId).relativize(fmd.filepath) }"}>
      <dcterms:type>{fmd.vocabulary.vocabulary}</dcterms:type>
      <dcterms:format>{fmd.mimeType}</dcterms:format>
      <dcterms:title>{fmd.title}</dcterms:title>
      <accessibleToRights>{fmd.accessibleTo}</accessibleToRights>
      {fmd.subtitles.map(subtitleXml(depositId))}
    </file>
  }

  private def subtitleXml(depositId: DepositId)(subtitle: Subtitles): Elem = {
    val filepath = depositDir(depositId).relativize(subtitle.path)

    subtitle.language
      .map(lang => <dcterms:relation xml:lang={lang}>{s"data/$filepath"}</dcterms:relation>)
      .getOrElse(<dcterms:relation>{s"data/$filepath"}</dcterms:relation>)
  }
}
