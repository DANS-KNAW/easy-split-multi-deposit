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

import scala.util.{ Failure, Success, Try }
import nl.knaw.dans.lib.error.{ CompositeException, TraversableTryExtensions }

import scala.collection.mutable

/**
 * An action to be performed by Process SIP. It provides three methods that can be invoked to verify
 * the feasibility of the action, to perform the action and - if necessary - to roll back the action.
 */
trait Action {
  self =>

  /**
   * Verifies whether all preconditions are met for this specific action.
   *
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  def checkPreconditions: Try[Unit] = Success(())

  /**
   * Exectue the action.
   *
   * @return `Success` if the execution was successful, `Failure` otherwise
   */
  def execute(): Try[Unit]

  /**
   * Cleans up results of a previous call to run so that a new call to run will not fail because of those results.
   *
   * @return `Success` if the rollback was successful, `Failure` otherwise
   */
  def rollback(): Try[Unit] = Success(())

  /**
   * Run an action. First the precondition is checked. If it fails a `PreconditionsFailedException`
   * with a report is returned. Else, if the precondition succeed, the action is executed.
   * If this fails, the action is rolled back and a `ActionRunFailedException` with a report is returned.
   * If the execution was successful, `Success` is returned
   *
   * @return `Success` if the full execution was successful, `Failure` otherwise
   */
  def run: Try[Unit] = {
    for {
      _ <- checkPreconditions.recoverWith {
        case e: Exception =>
          Failure(PreconditionsFailedException(generateReport("Precondition failures:", e, "Due to these errors in the preconditions, nothing was done.")))
      }
      _ <- execute().recoverWith {
        case e: Exception => List(Failure(e), rollback()).collectResults.recoverWith {
          case e: CompositeException => Failure(ActionRunFailedException(generateReport("Errors in Multi-Deposit Instructions file:", e)))
        }
      }
    } yield ()
  }

  private def generateReport[T](header: String = "", t: Throwable, footer: String = ""): String = {
    val report = t match {
      case ActionException(row, msg, _) => s" - row $row: $msg"
      case CompositeException(ths) =>
        ths.toSeq
          .collect { case ActionException(row, msg, _) => s" - row $row: $msg" }
          .mkString("\n")
      case e => s" - unexpected error: ${e.getMessage}"
    }

    header.toOption.map(_ + "\n").getOrElse("") + report + footer.toOption.map("\n" + _).getOrElse("")
  }

  /**
   * Compose this action with `other` into a new action such that it runs both actions in the correct order.
   * [[checkPreconditions]] and [[execute]] are run in order, [[rollback]] is run in reversed order.
   *
   * @param other the second action to be run
   * @return a composed action
   */
  def compose(other: Action): Action = new Action {
    private val stack = mutable.Stack[Action]()

    override def checkPreconditions: Try[Unit] = {
      List(self, other)
        .map(_.checkPreconditions)
        .collectResults
        .map(_ => ())
    }

    override def execute(): Try[Unit] = {
      stack.push(self)
      for {
        _ <- self.execute()
        _ = stack.push(other)
        _ <- other.execute()
      } yield ()
    }

    override def rollback(): Try[Unit] = {
      Stream.continually(stack.isEmpty)
        .takeWhile(empty => !empty)
        .map(_ => stack.pop().rollback())
        .toList
        .collectResults
        .map(_ => ())
    }
  }
}

object Action {
  def empty: Action = new Action {
    def execute(): Try[Unit] = Success(())
  }

  def apply(precondition: () => Try[Unit],
            action: () => Try[Unit],
            undo: () => Try[Unit]): Action = new Action {
    override def checkPreconditions: Try[Unit] = precondition()
    override def execute(): Try[Unit] = action()
    override def rollback(): Try[Unit] = undo()
  }
}
