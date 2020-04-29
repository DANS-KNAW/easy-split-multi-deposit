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

  def fileDescriptor(depositId: DepositId)(row: DepositRow): Option[Validated[(Int, File, Option[String], Option[FileAccessRights], Option[FileAccessRights])]] = {
    val path = row.find(Headers.FilePath).map(findRegularFile(depositId, row.rowNum))
    val title = row.find(Headers.FileTitle)
    val accessibility = row.find(Headers.FileAccessibility).map(fileAccessibility(row.rowNum))
    val visibility = row.find(Headers.FileVisibility).map(fileVisibility(row.rowNum))

    (path, title, accessibility, visibility) match {
      case (None, None, None, None) => None
      case (None, _, a, v) =>
        val err = ParseError(row.rowNum, s"${ Headers.FileTitle }, ${ Headers.FileAccessibility } and ${ Headers.FileVisibility } are only allowed if ${ Headers.FilePath } is also given")

        (
          a.sequence,
          v.sequence,
        ).tupled
          .fold(e => (err +: e).invalid, _ => err.toInvalid)
          .some
      case (Some(p), t, a, v) =>
        (
          row.rowNum.toValidated,
          p,
          t.toValidated,
          a.sequence,
          v.sequence,
        ).tupled.some
    }
  }

  private def combineFileDescriptors(rowNum: Int)(descriptors: List[(Int, File, Option[String], Option[FileAccessRights], Option[FileAccessRights])]): Validated[Map[File, FileDescriptor]] = {
    descriptors.groupBy { case (_, file, _, _, _) => file }
      .map((toFileDescriptor(rowNum) _).tupled)
      .toList
      .sequence
      .map(_.toMap)
  }

  private def checkAtMostOneElementInList[T](list: List[T])(err: List[T] => ParseError): Validated[Option[T]] = {
    list match {
      case Nil => none.toValidated
      case title :: Nil => title.some.toValidated
      case multipleTitles => err(multipleTitles).toInvalid
    }
  }

  private def toFileDescriptor(rowNum: => Int)(file: File, dataPerPath: List[(Int, File, Option[String], Option[FileAccessRights], Option[FileAccessRights])]): Validated[(File, FileDescriptor)] = {
    (
      dataPerPath.collect { case (localRowNum, _, _, _, _) => localRowNum }.min.toValidated,
      checkAtMostOneElementInList(dataPerPath.collect { case (_, _, Some(title), _, _) => title })(titles => ParseError(rowNum, s"${ Headers.FileTitle } defined multiple values for file '$file': ${ titles.mkString("[", ", ", "]") }")),
      checkAtMostOneElementInList(dataPerPath.collect { case (_, _, _, Some(far), _) => far })(fileAccessibilities => ParseError(rowNum, s"${ Headers.FileAccessibility } defined multiple values for file '$file': ${ fileAccessibilities.mkString("[", ", ", "]") }")),
      checkAtMostOneElementInList(dataPerPath.collect { case (_, _, _, _, Some(fv)) => fv })(fileVisibilities => ParseError(rowNum, s"${ Headers.FileVisibility } defined multiple values for file '$file': ${ fileVisibilities.mkString("[", ", ", "]") }")),
    ).mapN(FileDescriptor)
      .map((file, _))
      .andThen {
        case (_, FileDescriptor(localRowNum, _, Some(as), Some(vs))) if vs > as => ParseError(localRowNum, s"${ Headers.FileVisibility } ($vs) is more restricted than ${ Headers.FileAccessibility } ($as) for file '$file'. (User will potentially have access to an invisible file.)").toInvalid
        case otherwise => otherwise.toValidated
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
