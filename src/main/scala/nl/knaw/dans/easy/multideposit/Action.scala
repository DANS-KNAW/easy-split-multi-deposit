/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * An action to be performed by Process SIP. It provides three methods that can be invoked to verify
 * the feasibility of the action, to perform the action and - if necessary - to roll back the action.
 */
/*
  To future developers: this class is a semigroup. It satisfies the associativity law as defined
  in https://wiki.haskell.org/Typeclassopedia#Laws_4.
 */
trait Action extends DebugEnhancedLogging {

  private def logPreconditions(): Unit = {
    logger.info(s"Checking preconditions of ${getClass.getSimpleName} ...")
  }
  private def logExecute(): Unit = {
    logger.info(s"Executing action of ${getClass.getSimpleName} ...")
  }
  private def logRollback(): Unit = {
    logger.info(s"An error occurred. Rolling back action ${getClass.getSimpleName} ...")
  }
  private[Action] def innerCheckPreconditions: Try[Unit] = Try(logPreconditions()).flatMap(_ => checkPreconditions)
  private[Action] def innerExecute(): Try[Unit] = Try(logExecute()).flatMap(_ => execute())
  private[Action] def innerRollback(): Try[Unit] = Try(logRollback()).flatMap(_ => rollback())

  /**
   * Verifies whether all preconditions are met for this specific action.
   *
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  protected def checkPreconditions: Try[Unit] = Success(())

  /**
   * Exectue the action.
   *
   * @return `Success` if the execution was successful, `Failure` otherwise
   */
  protected def execute(): Try[Unit]

  /**
   * Cleans up results of a previous call to run so that a new call to run will not fail because of those results.
   *
   * @return `Success` if the rollback was successful, `Failure` otherwise
   */
  protected def rollback(): Try[Unit] = Success(())

  /**
   * Run an action. First the precondition is checked. If it fails a `PreconditionsFailedException`
   * with a report is returned. Else, if the precondition succeed, the action is executed.
   * If this fails, the action is rolled back and a `ActionRunFailedException` with a report is returned.
   * If the execution was successful, `Success` is returned
   *
   * @return `Success` if the full execution was successful, `Failure` otherwise
   */
  final def run: Try[Unit] = {
    for {
      _ <- innerCheckPreconditions.recoverWith {
        case NonFatal(e) =>
          Failure(PreconditionsFailedException(
            report = generateReport(
              header = "Precondition failures:",
              throwable = e,
              footer = "Due to these errors in the preconditions, nothing was done."),
            cause = e))
      }
      _ <- innerExecute().recoverWith {
        case NonFatal(e) =>
          List(Failure(e), innerRollback())
            .collectResults
            .recoverWith {
              case e: CompositeException => Failure(ActionRunFailedException(
                report = generateReport(
                  header = "Failures during the execute phase:",
                  throwable = e,
                  footer = "The actions that were already performed, were rolled back."),
                cause = e))
            }
      }
    } yield ()
  }

  private def generateReport(header: String = "", throwable: Throwable, footer: String = ""): String = {
    val report = throwable match {
      case ActionException(row, msg, _) => s" - row $row: $msg"
      case CompositeException(throwables) =>
        throwables.toSeq
          .collect {
            case ActionException(row, msg, _) => s" - row $row: $msg"
            case NonFatal(e) => s" - unexpected error: ${e.getMessage}"
          }
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
  def compose(other: Action): ComposedAction = {
    other match {
      case composedAction: Action#ComposedAction => ComposedAction(this :: composedAction.actions)
      case action => ComposedAction(this :: action :: Nil)
    }
  }

  final case class ComposedAction(actions: List[Action]) extends Action {
    private lazy val executedActions = mutable.Stack[Action]()

    override private[Action] def innerCheckPreconditions: Try[Unit] = checkPreconditions
    override private[Action] def innerExecute(): Try[Unit] = execute()
    override private[Action] def innerRollback(): Try[Unit] = rollback()

    override protected def checkPreconditions: Try[Unit] = {
      actions.map(_.innerCheckPreconditions).collectResults.map(_ => ())
    }

    override protected def execute(): Try[Unit] = {
      actions.foldLeft(Try { () })((res, action) => res.flatMap(_ => {
        executedActions.push(action)
        action.innerExecute()
      }))
    }

    override protected def rollback(): Try[Unit] = {
      Stream.continually(executedActions.nonEmpty)
        .takeWhile(_ == true)
        .map(_ => executedActions.pop().innerRollback())
        .toList
        .collectResults
        .map(_ => ())
    }

    override def compose(other: Action): ComposedAction = {
      other match {
        case composedAction: Action#ComposedAction => ComposedAction(actions ++ composedAction.actions)
        case action => ComposedAction(actions :+ action)
      }
    }

    override def equals(obj: Any): Boolean = {
      obj match {
        case ca: Action#ComposedAction => this.actions == ca.actions
        case _ => false
      }
    }
  }
}

object Action {
  def apply(precondition: () => Try[Unit],
            action: () => Try[Unit],
            undo: () => Try[Unit]): Action = new Action {
    override protected def checkPreconditions: Try[Unit] = precondition()
    override protected def execute(): Try[Unit] = action()
    override protected def rollback(): Try[Unit] = undo()
  }
}
