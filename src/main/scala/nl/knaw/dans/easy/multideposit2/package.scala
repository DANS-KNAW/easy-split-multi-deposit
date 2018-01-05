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

import java.nio.charset.Charset

import org.apache.commons.io.Charsets

import scala.util.Try

package object multideposit2 {

  val encoding: Charset = Charsets.UTF_8

  implicit class SeqExtensions[T](val seq: Seq[T]) extends AnyVal {
    def mapUntilFailure[S](f: T => Try[S]): Try[Seq[S]] = Try { seq.toStream.map(f(_).get) }
  }
}
