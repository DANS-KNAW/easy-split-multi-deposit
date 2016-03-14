/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit

import scala.util.{Success, Try}

/**
  * An action to be performed by Process SIP. It provides three methods that can be invoked to verify
  * the feasibility of the action, to perform the action and - if necessary - to roll back the action.
  */
trait Action {

  /**
    * Verifies whether all preconditions are met for this specific action.
    *
    * @return `Success` when all preconditions are met, `Failure` otherwise
    */
  def checkPreconditions: Try[Unit] = Success(())

  /**
    * Exectue the action.
    *
    * @return `Success` if the execution was successfull, `Failure` otherwise
    */
  def run(): Try[Unit]

  /**
    * Cleans up results of a previous call to run so that a new call to run will not fail because of those results.
    *
    * @return TODO
    */
  def rollback(): Try[Unit] = Success(())
}
