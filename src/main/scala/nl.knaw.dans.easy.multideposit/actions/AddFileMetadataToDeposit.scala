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

import nl.knaw.dans.easy.multideposit.BetterFileExtensions
import nl.knaw.dans.easy.multideposit.PathExplorer.{ InputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.control.NonFatal
import scala.util.{ Failure, Try }
import scala.xml.{ Elem, NodeSeq }

class AddFileMetadataToDeposit extends DebugEnhancedLogging {

  def addFileMetadata(depositId: DepositId, fileMetadata: Seq[FileMetadata])(implicit input: InputPathExplorer, stage: StagingPathExplorer): Try[Unit] = Try {
    logger.debug(s"add file metadata for $depositId")

    stage.stagingFileMetadataFile(depositId).writeXml(depositToFileXml(depositId, fileMetadata))
  } recoverWith {
    case NonFatal(e) => Failure(ActionException(s"Could not write file metadata", e))
  }

  private def depositToFileXml(depositId: DepositId, fileMetadata: Seq[FileMetadata])(implicit input: InputPathExplorer): Elem = {
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

  private def fileXmls(depositId: DepositId, fmds: Seq[FileMetadata])(implicit input: InputPathExplorer): Seq[Elem] = {
    fmds.map {
      case av: AVFileMetadata => avFileXml(depositId, av)
      case fmd: DefaultFileMetadata => defaultFileXml(depositId, fmd)
    }
  }

  private def defaultFileXml(depositId: DepositId, fmd: DefaultFileMetadata)(implicit input: InputPathExplorer): Elem = {
    <file filepath={s"data/${ input.depositDir(depositId).relativize(fmd.filepath) }"}>
      <dcterms:format>{fmd.mimeType}</dcterms:format>
      {fmd.title.map(title => <dcterms:title>{title}</dcterms:title>).getOrElse(NodeSeq.Empty)}
      {fmd.accessibleTo.map(act => <accessibleToRights>{act}</accessibleToRights>).getOrElse(NodeSeq.Empty)}
      {fmd.visibleTo.map(act => <visibleToRights>{act}</visibleToRights>).getOrElse(NodeSeq.Empty)}
    </file>
  }

  private def avFileXml(depositId: DepositId, fmd: AVFileMetadata)(implicit input: InputPathExplorer): Elem = {
    <file filepath={s"data/${ input.depositDir(depositId).relativize(fmd.filepath) }"}>
      <dcterms:type>{fmd.vocabulary.vocabulary}</dcterms:type>
      <dcterms:format>{fmd.mimeType}</dcterms:format>
      <dcterms:title>{fmd.title}</dcterms:title>
      <accessibleToRights>{fmd.accessibleTo}</accessibleToRights>
      <visibleToRights>{fmd.visibleTo}</visibleToRights>
      {fmd.subtitles.map(subtitleXml(depositId))}
    </file>
  }

  private def subtitleXml(depositId: DepositId)(subtitle: Subtitles)(implicit input: InputPathExplorer): Elem = {
    val filepath = input.depositDir(depositId).relativize(subtitle.file)

    subtitle.language
      .map(lang => <dcterms:relation xml:lang={lang}>{s"data/$filepath"}</dcterms:relation>)
      .getOrElse(<dcterms:relation>{s"data/$filepath"}</dcterms:relation>)
  }
}
