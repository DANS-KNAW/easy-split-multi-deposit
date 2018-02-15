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

package object actions {

  class ActionException(msg: String, cause: Option[Throwable] = None) extends Exception(msg, cause.orNull)
  object ActionException {
    def apply(msg: String, cause: Throwable): ActionException = new ActionException(msg, Option(cause))
    def apply(msg: String): ActionException = new ActionException(msg)
    def unapply(arg: ActionException): Option[(String, Throwable)] = Some((arg.getMessage, arg.getCause))
  }

  case class InvalidDatamanagerException(msg: String) extends Exception(msg)
  case class InvalidInputException(row: Int, msg: String) extends Exception(msg)
}
