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
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.traverse._
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.model.{ DepositId, MultiDepositKey, NonEmptyList, listToNEL }
import nl.knaw.dans.lib.string._
import org.joda.time.DateTime

trait ParserUtils {
  this: InputPathExplorer =>

  def getRowNum(row: DepositRow): Int = row("ROW").toInt

  def extractExactlyOne(rowNum: Int, name: MultiDepositKey, rows: DepositRows): FailFast[String] = {
    rows.flatMap(_.find(name)).distinct match {
      case Seq() => ParseError(rowNum, s"There should be one non-empty value for $name").asLeft
      case Seq(t) => t.asRight
      case ts => ParseError(rowNum, s"Only one row is allowed to contain a value for the column '$name'. Found: ${ ts.mkString("[", ", ", "]") }").asLeft
    }
  }

  def extractAtLeastOne(rowNum: Int, name: MultiDepositKey, rows: DepositRows): FailFast[NonEmptyList[String]] = {
    rows.flatMap(_.find(name)).distinct match {
      case Seq() => ParseError(rowNum, s"There should be at least one non-empty value for $name").asLeft
      case xs => listToNEL(xs.toList).asRight
    }
  }

  def extractAtMostOne(rowNum: Int, name: MultiDepositKey, rows: DepositRows): FailFast[Option[String]] = {
    rows.flatMap(_.find(name)).distinct match {
      case Seq() => none.asRight
      case Seq(t) => t.some.asRight
      case ts => ParseError(rowNum, s"At most one row is allowed to contain a value for the column '$name'. Found: ${ ts.mkString("[", ", ", "]") }").asLeft
    }
  }

  def extractList[T](rows: DepositRows)(f: (=> Int) => DepositRow => Option[Validated[T]]): Validated[List[T]] = {
    rows.flatMap(row => f(getRowNum(row))(row)).toList.sequence
  }

  def extractList(rows: DepositRows, name: MultiDepositKey): List[String] = {
    rows.flatMap(_.find(name)).toList
  }

  def checkValidChars(value: String, rowNum: => Int, column: => MultiDepositKey): FailFast[String] = {
    val invalidCharacters = "[^a-zA-Z0-9_-]".r.findAllIn(value).toSeq.distinct
    if (invalidCharacters.isEmpty) value.asRight
    else ParseError(rowNum, s"The column '$column' contains the following invalid characters: ${ invalidCharacters.mkString("{", ", ", "}") }").asLeft
  }

  def date(rowNum: => Int, columnName: => String)(s: String): FailFast[DateTime] = {
    Either.catchOnly[IllegalArgumentException] { DateTime.parse(s) }
      .leftMap(e => ParseError(rowNum, s"$columnName value '$s' does not represent a date"))
  }

  def missingRequired[T](rowNum: Int, row: DepositRow, required: Set[String]): ParseError = {
    val blankRequired = row.collect { case (key, value) if value.isBlank && required.contains(key) => key }
    val missingColumns = required.diff(row.keySet)
    val missing = blankRequired.toSet ++ missingColumns
    require(missing.nonEmpty, "the list of missing elements is supposed to be non-empty")

    missing.toList match {
      case value :: Nil => ParseError(rowNum, s"Missing value for: $value")
      case values => ParseError(rowNum, s"Missing value(s) for: ${ values.mkString("[", ", ", "]") }")
    }
  }

  /**
   * Returns the absolute file for the given `path`. If the input is correct, `path` is relative
   * to the deposit it is in.
   *
   * If both options do not suffice, the path is just wrapped in a `File`.
   *
   * @param path the path to a file, as provided by the user input
   * @return the absolute path to this file, if it exists
   */
  def findRegularFile(depositId: DepositId, rowNum: => Int)(path: String): FailFast[File] = {
    val file = depositDir(depositId) / path

    file match {
      case f if f.isRegularFile => f.asRight
      case f if f.exists => ParseError(rowNum, s"path '$path' exists, but is not a regular file").asLeft
      case _ => ParseError(rowNum, s"unable to find path '$path'").asLeft
    }
  }
}
