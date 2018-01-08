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

import java.nio.file.{ Files, Path, Paths }
import java.util.Locale

import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.string.StringExtensions

import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

trait ParserUtils extends DebugEnhancedLogging {

  def getRowNum(row: DepositRow): Int = row("ROW").toInt

  def extractNEL[T](rows: DepositRows)(f: (=> Int) => DepositRow => Option[Try[T]]): Try[NonEmptyList[T]] = {
    rows.flatMap(row => f(getRowNum(row))(row)).collectResults.map(_.toList)
  }

  def extractNEL(rows: DepositRows, rowNum: Int, name: MultiDepositKey): Try[NonEmptyList[String]] = {
    rows.flatMap(_.find(name)) match {
      case Seq() => Failure(ParseException(rowNum, s"There should be at least one non-empty value for $name"))
      case xs => Try { xs.toList }
    }
  }

  def extractList[T](rows: DepositRows)(f: (=> Int) => DepositRow => Option[Try[T]]): Try[List[T]] = {
    rows.flatMap(row => f(getRowNum(row))(row)).collectResults.map(_.toList)
  }

  def extractList(rows: DepositRows, name: MultiDepositKey): List[String] = {
    rows.flatMap(_.find(name)).toList
  }

  def atMostOne[T](rowNum: => Int, columnNames: => NonEmptyList[MultiDepositKey])(values: List[T]): Try[Option[T]] = {
    values.distinct match {
      case Nil => Success(None)
      case t :: Nil => Success(Some(t))
      case ts => checkMultipleValues(rowNum, columnNames, ts)
    }
  }

  def exactlyOne[T](rowNum: => Int, columnNames: => NonEmptyList[MultiDepositKey])(values: List[T]): Try[T] = {
    values.distinct match {
      case Nil =>
        columnNames match {
          case name :: Nil => Failure(ParseException(rowNum, "One row has to contain " +
            s"a value for the column: '$name'"))
          case names => Failure(ParseException(rowNum, "One row has to contain a value for these " +
            s"columns: ${ names.mkString("[", ", ", "]") }"))
        }
      case t :: Nil => Success(t)
      case ts => checkMultipleValues(rowNum, columnNames, ts)
    }
  }

  private def checkMultipleValues[T](rowNum: Int, columnNames: NonEmptyList[MultiDepositKey], ts: List[Any]) = {
    columnNames match {
      case name :: Nil => Failure(ParseException(rowNum, "Only one row is allowed " +
        s"to contain a value for the column '$name'. Found: ${ ts.mkString("[", ", ", "]") }"))
      case names => Failure(ParseException(rowNum, "Only one row is allowed to contain a value for " +
        s"these columns: ${ names.mkString("[", ", ", "]") }. Found: ${ ts.mkString("[", ", ", "]") }"))
    }
  }

  def checkValidChars(value: String, rowNum: => Int, column: => MultiDepositKey): Try[String] = {
    val invalidCharacters = "[^a-zA-Z0-9_-]".r.findAllIn(value).toSeq.distinct
    if (invalidCharacters.isEmpty) Success(value)
    else Failure(ParseException(rowNum, s"The column '$column' contains the following invalid characters: ${ invalidCharacters.mkString("{", ", ", "}") }"))
  }

  def missingRequired[T](rowNum: Int, row: DepositRow, required: Set[String]): Failure[T] = {
    val blankRequired = row.collect { case (key, value) if value.isBlank && required.contains(key) => key }
    val missingColumns = required.diff(row.keySet)
    val missing = blankRequired.toSet ++ missingColumns
    require(missing.nonEmpty, "the list of missing elements is supposed to be non-empty")

    missing.toList match {
      case value :: Nil => Failure(ParseException(rowNum, s"Missing value for: $value"))
      case values => Failure(ParseException(rowNum, s"Missing value(s) for: ${ values.mkString("[", ", ", "]") }"))
    }
  }

  def isValidISO639_1Language(lang: String): Boolean = {
    val b0: Boolean = lang.length == 2
    val b1: Boolean = new Locale(lang).getDisplayLanguage.toLowerCase != lang.toLowerCase

    b0 && b1
  }

  private lazy val iso639v2Languages = Locale.getISOLanguages.map(new Locale(_).getISO3Language).toSet

  def iso639_2Language(columnName: MultiDepositKey)(rowNum: => Int)(row: DepositRow): Option[Try[String]] = {
    row.find(columnName)
      .map(lang => {
        // Most ISO 639-2/T languages are contained in the iso639v2Languages Set.
        // However, some of them are not and need to be checked using the second predicate.
        // The latter also allows to check ISO 639-2/B language codes.
        lazy val b0 = lang.length == 3
        lazy val b1 = iso639v2Languages.contains(lang)
        lazy val b2 = new Locale(lang).getDisplayLanguage.toLowerCase != lang.toLowerCase

        if (b0 && (b1 || b2)) Success(lang)
        else Failure(ParseException(rowNum, s"Value '$lang' is not a valid value for $columnName"))
      })
  }

  /**
   * Returns the absolute file for the given `path`. If the input is correct, `path` is relative
   * to the deposit it is in.
   *
   * By means of backwards compatibility, the `path` might also be
   * relative to the multideposit. In this case the correct absolute file is returned as well,
   * besides which a warning is logged, notifying the user that `path` should be relative to the
   * deposit instead.
   *
   * If both options do not suffice, the path is just wrapped in a `File`.
   *
   * @param path the path to a file, as provided by the user input
   * @return the absolute path to this file, if it exists
   */
  def findPath(depositId: DepositId)(path: String)(implicit settings: Settings): Path = {
    val option1 = multiDepositDir(depositId).resolve(path)
    val option2 = settings.multidepositDir.resolve(path)

    (option1, option2) match {
      case (path1, _) if Files.exists(path1) => path1
      case (_, path2) if Files.exists(path2) =>
        logger.warn(s"path '$path' is not relative to its depositId '$depositId', but rather relative to the multideposit")
        path2
      case (_, _) => Paths.get(path)
    }
  }
}
