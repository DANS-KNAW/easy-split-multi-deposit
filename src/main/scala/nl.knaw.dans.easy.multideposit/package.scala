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

import better.files.File
import org.apache.commons.io.Charsets
import org.joda.time.{ DateTime, DateTimeZone }
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }

import scala.collection.generic.CanBuildFrom
import scala.util.{ Failure, Success, Try }
import scala.xml.{ Elem, PrettyPrinter, Utility, XML }

package object multideposit {
  val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateTime()
  def now: String = DateTime.now(DateTimeZone.UTC).toString(dateTimeFormatter)

  val encoding: Charset = Charsets.UTF_8

  case class DepositPermissions(permissions: String, group: String)

  /**
   * An exception caused by the user in some way, for example by passing in erroneous arguments or an invalid
   * instructions.csv
   *
   * @param msg the error message
   */
  class UserException(msg: String) extends Exception(msg)

  implicit class SeqExtensions[T](val seq: Seq[T]) extends AnyVal {
    def mapUntilFailure[S](f: T => Try[S])(implicit cbf: CanBuildFrom[Seq[T], S, Seq[S]]): Try[Seq[S]] = {
      val bf = cbf()
      for (t <- seq) {
        f(t) match {
          case Success(x) => bf += x
          case Failure(e) => return Failure(e)
        }
      }
      Success(bf.result())
    }
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
