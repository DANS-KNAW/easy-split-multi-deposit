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
package nl.knaw.dans.easy

import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.Paths

import better.files.File
import cats.data.EitherNec
import org.apache.commons.io.Charsets
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }
import org.joda.time.{ DateTime, DateTimeZone }

import scala.xml.{ Elem, PrettyPrinter, Utility, XML }

package object multideposit {
  type FailFast[T] = Either[ConversionFailed, T]
  type FailFastNec[T] = EitherNec[ConversionFailed, T]

  val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateTime()

  def now: String = DateTime.now(DateTimeZone.UTC).toString(dateTimeFormatter)

  val encoding: Charset = Charsets.UTF_8

  val licensesDir = Paths.get("target/easy-licenses/licenses")

  case class DepositPermissions(permissions: String, group: String)

  sealed trait SmdError {
    val msg: String
    val cause: Option[Throwable] = None
  }

  case class ParseFailed(override val msg: String) extends SmdError

  sealed trait ConversionFailed extends SmdError

  case class ActionError(override val msg: String, override val cause: Option[Throwable] = None) extends ConversionFailed
  object ActionError {
    def apply(msg: String, cause: Throwable): ActionError = new ActionError(msg, Option(cause))
  }

  case class InvalidDatamanager(override val msg: String) extends ConversionFailed

  case class InvalidInput(row: Int, localMsg: String) extends ConversionFailed {
    override val msg = s"row $row: $localMsg"
  }

  implicit class BetterFileExtensions(val file: File) extends AnyVal {
    /**
     * Writes the xml to `file` and prepends a simple xml header: `<?xml version="1.0" encoding="UTF-8"?>`
     *
     * @param elem     the xml to be written
     * @param encoding the encoding applied to this xml
     */
    @throws[IOException]("in case of an I/O error")
    def writeXml(elem: Elem, encoding: Charset = encoding): Unit = {
      file.parent.createDirectories()
      XML.save(file.toString, XML.loadString(new PrettyPrinter(160, 2).format(Utility.trim(elem))), encoding.toString, xmlDecl = true)
    }
  }
}
