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
import cats.instances.list._
import cats.instances.option._
import cats.syntax.apply._
import cats.syntax.option._
import cats.syntax.traverse._
import cats.syntax.validated._
import nl.knaw.dans.easy.multideposit.model.FileAccessRights.FileAccessRights
import nl.knaw.dans.easy.multideposit.model.{ DepositId, FileAccessRights, FileDescriptor }

trait FileDescriptorParser {
  this: ParserUtils =>

  def extractFileDescriptors(depositId: DepositId, rowNum: Int, rows: DepositRows): Validated[Map[File, FileDescriptor]] = {
    extractList(rows)(fileDescriptor(depositId))
      .fold(_.invalid, combineFileDescriptors(rowNum))
  }

  def fileDescriptor(depositId: DepositId)(row: DepositRow): Option[Validated[(File, Option[String], Option[FileAccessRights], Option[FileAccessRights])]] = {
    val path = row.find("FILE_PATH").map(findRegularFile(depositId, row.rowNum))
    val title = row.find("FILE_TITLE")
    val accessibility = row.find("FILE_ACCESSIBILITY").map(fileAccessibility(row.rowNum))
    val visibility = row.find("FILE_VISIBILITY").map(fileVisibility(row.rowNum))

    (path, title, accessibility, visibility) match {
      case (None, None, None, None) => None
      case (None, _, a, v) => Some {
        val err = ParseError(row.rowNum, "FILE_TITLE, FILE_ACCESSIBILITY and FILE_VISIBILITY are only allowed if FILE_PATH is also given")

        (
          a.sequence[Validated, FileAccessRights],
          v.sequence[Validated, FileAccessRights],
        ).tupled
          .fold(e => (err +: e).invalid, _ => err.toInvalid)
      }
      case (Some(p), t, a, v) => Some {
        (
          p,
          t.toValidated,
          a.sequence[Validated, FileAccessRights],
          v.sequence[Validated, FileAccessRights],
        ).tupled
      }
    }
  }

  private def combineFileDescriptors(rowNum: Int)(descriptors: List[(File, Option[String], Option[FileAccessRights], Option[FileAccessRights])]): Validated[Map[File, FileDescriptor]] = {
    descriptors.groupBy { case (file, _, _, _) => file }
      .map((toFileDescriptor(rowNum) _).tupled)
      .toList
      .sequence
      .map(_.toMap)
  }

  private def ensureAtMostOneElementInList[T](list: List[T])(err: List[T] => ParseError): Validated[Option[T]] = {
    list match {
      case Nil => none.toValidated
      case title :: Nil => title.some.toValidated
      case multipleTitles => err(multipleTitles).toInvalid
    }
  }

  private def toFileDescriptor(rowNum: => Int)(file: File, dataPerPath: List[(File, Option[String], Option[FileAccessRights], Option[FileAccessRights])]): Validated[(File, FileDescriptor)] = {
    (
      ensureAtMostOneElementInList(dataPerPath.collect { case (_, Some(title), _, _) => title })(titles => ParseError(rowNum, s"FILE_TITLE defined multiple values for file '$file': ${ titles.mkString("[", ", ", "]") }")),
      ensureAtMostOneElementInList(dataPerPath.collect { case (_, _, Some(far), _) => far })(fileAccessibilities => ParseError(rowNum, s"FILE_ACCESSIBILITY defined multiple values for file '$file': ${ fileAccessibilities.mkString("[", ", ", "]") }")),
      ensureAtMostOneElementInList(dataPerPath.collect { case (_, _, _, Some(fv)) => fv })(fileVisibilities => ParseError(rowNum, s"FILE_VISIBILITY defined multiple values for file '$file': ${ fileVisibilities.mkString("[", ", ", "]") }")),
    ).mapN(FileDescriptor)
      .map((file, _))
      .andThen {
        case (_, FileDescriptor(_, Some(as), Some(vs))) if vs >= as => ParseError(rowNum, s"FILE_VISIBILITY ($vs) is more restricted than FILE_ACCESSIBILITY ($as) for file '$file'. (User will potentially have access to an invisible file.)").toInvalid
        case tuple => tuple.toValidated
      }
  }

  def fileAccessibility(rowNum: => Int)(role: String): Validated[FileAccessRights] = {
    FileAccessRights.valueOf(role)
      .toValidNec(ParseError(rowNum, s"Value '$role' is not a valid file accessright"))
  }

  def fileVisibility(rowNum: => Int)(role: String): Validated[FileAccessRights] = {
    FileAccessRights.valueOf(role)
      .toValidNec(ParseError(rowNum, s"Value '$role' is not a valid file visibility"))
  }
}
