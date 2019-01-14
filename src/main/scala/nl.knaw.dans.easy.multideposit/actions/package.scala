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
package nl.knaw.dans.easy.multideposit

import better.files.File

package object actions {

  type FailFast[T] = Either[CreateDepositError, T]

  sealed abstract class CreateDepositError(msg: String, cause: Option[Throwable] = None) extends Exception(msg, cause.orNull)

  class ActionException(msg: String, cause: Option[Throwable] = None) extends CreateDepositError(msg, cause)
  object ActionException {
    def apply(msg: String, cause: Throwable): ActionException = new ActionException(msg, Option(cause))
    def apply(msg: String): ActionException = new ActionException(msg)
    def unapply(arg: ActionException): Option[(String, Throwable)] = Some((arg.getMessage, arg.getCause))
  }
  case class InvalidDatamanagerException(msg: String) extends CreateDepositError(msg)
  case class InvalidInputException(row: Int, msg: String) extends CreateDepositError(s"row $row: $msg")
  case class FfprobeErrorException(file: File, exitValue: Int, err: String) extends CreateDepositError(s"File '$file' could not be probed. Exit value: $exitValue, STDERR: '$err'")
}
