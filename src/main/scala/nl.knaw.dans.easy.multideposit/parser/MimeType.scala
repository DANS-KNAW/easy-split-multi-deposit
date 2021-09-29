/*
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

import java.io.IOException

import better.files.File
import cats.data.Validated
import nl.knaw.dans.easy.multideposit.model.MimeType
import org.apache.tika.Tika

object MimeType {

  private val tika = new Tika

  /**
   * Identify the mimeType of a path.
   *
   * @param file the file to identify
   * @return the mimeType of the path if the identification was successful; `Failure` otherwise
   */
  def get(file: File): Validated[MimeType] = {
    Validated.catchOnly[IOException] { tika.detect(file.path) }
      .leftMap(ioe => ParseError(-1, ioe.getMessage).chained)
  }
}
