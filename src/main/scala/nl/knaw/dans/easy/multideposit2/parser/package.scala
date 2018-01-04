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
package nl.knaw.dans.easy.multideposit2

import java.nio.file.Path

import nl.knaw.dans.easy.multideposit2.model.MultiDepositKey
import nl.knaw.dans.easy.multideposit.StringExtensions

package object parser {

  type DepositRow = Map[MultiDepositKey, String]
  type DepositRows = Seq[DepositRow]

  implicit class DatasetRowFind(val row: DepositRow) extends AnyVal {
    def find(name: MultiDepositKey): Option[String] = row.get(name).filterNot(_.isBlank)
  }

  case class EmptyInstructionsFileException(path: Path) extends Exception(s"The given instructions file in '$path' is empty")
  case class ParseException(row: Int, message: String, cause: Throwable = null) extends Exception(message, cause)
  case class ParserFailedException(report: String, cause: Throwable = null) extends Exception(report, cause)
}
