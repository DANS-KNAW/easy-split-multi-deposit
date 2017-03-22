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

import scala.annotation.tailrec
import scala.util.control.NonFatal

/*
  To future developers: this class is a Category. It should satisfy the law of composition.
 */
trait Action[-A, +T] extends DebugEnhancedLogging { self =>

  private def logPreconditions(): Unit = {
    logger.info(s"Checking preconditions of ${getClass.getSimpleName} ...")
  }
  private def logExecute(): Unit = {
    logger.info(s"Executing action of ${getClass.getSimpleName} ...")
  }
  private def logRollback(): Unit = {
    logger.info(s"An error occurred. Rolling back action ${getClass.getSimpleName} ...")
  }
  protected[Action] def innerCheckPreconditions: Try[Unit] = Try(logPreconditions()).flatMap(_ => checkPreconditions)
  protected[Action] def innerExecute(a: A): Try[T] = Try(logExecute()).flatMap(_ => execute(a))
  protected[Action] def innerRollback(): Try[Unit] = Try(logRollback()).flatMap(_ => rollback())

  /**
   * Verifies whether all preconditions are met for this specific action.
   *
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  protected def checkPreconditions: Try[Unit] = Success(())

  /**
   * Exectue the action given an input `a`.
   *
   * @param a the action's input
   * @return `Success` if the execution was successful, `Failure` otherwise
   */
  protected def execute(a: A): Try[T]

  /**
   * Cleans up results of a previous call to run so that a new call to run will not fail because of those results.
   *
   * @return `Success` if the rollback was successful, `Failure` otherwise
   */
  protected def rollback(): Try[Unit] = Success(())

  /**
   * Run an action. First the precondition is checked. If it fails a `PreconditionsFailedException`
   * with a report is returned. Else, if the precondition succeed, the action is executed given the input `a`.
   * If this fails, the action is rolled back and a `ActionRunFailedException` with a report is returned.
   * If the execution was successful, `Success` is returned
   *
   * @param a the action's input
   * @return `Success` if the full execution was successful, `Failure` otherwise
   */
  def run(a: A): Try[T] = {
    def reportFailure(t: Throwable): Try[T] = {
      Failure(ActionRunFailedException(
        report = generateReport(
          header = "Errors in Multi-Deposit Instructions file:",
          throwable = t,
          footer = "The actions that were already performed, were rolled back."),
        cause = t
      ))
    }

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
      t <- innerExecute(a).recoverWith {
        case e1@CompositeException(es) =>
          rollback() match {
            case Success(_) => reportFailure(e1)
            case Failure(CompositeException(es2)) => reportFailure(CompositeException(es ++ es2))
            case Failure(e2) => reportFailure(CompositeException(es ++ List(e2)))
          }
        case NonFatal(e1) =>
          rollback() match {
            case Success(_) => reportFailure(e1)
            case Failure(CompositeException(es2)) => reportFailure(CompositeException(List(e1) ++ es2))
            case Failure(e2) => reportFailure(CompositeException(List(e1, e2)))
          }
      }
    } yield t
  }

  private def generateReport(header: String = "", throwable: Throwable, footer: String = ""): String = {

    @tailrec
    def report(es: List[Throwable], rpt: List[String] = Nil): List[String] = {
      es match {
        case Nil => rpt
        case ActionException(row, msg, _) :: xs => report(xs, s" - row $row: $msg" :: rpt)
        case CompositeException(ths) :: xs => report(ths.toList ::: xs, rpt)
        case NonFatal(ex) :: xs => report(xs, s" - unexpected error: ${ex.getMessage}" :: rpt)
      }
    }

    header.toOption.fold("")(_ + "\n") +
      report(List(throwable)).reverse.mkString("\n") +
      footer.toOption.fold("")("\n" + _)
  }

  /**
   * Sequentially composes two `Action`s by running their `execute` methods one after the other.
   * In this, the input of `other` is the output of this `Action`.
   *
   * @param other the `Action` to combine this `Action` with
   * @tparam S the output type of the second `Action`
   * @return an `Action` that composes these two actions sequentially
   */
  def combine[S](other: Action[T, S]): Action[A, S] = new Action[A, S] {
    private var pastLeft = false

    override def run(a: A): Try[S] = {
      pastLeft = false
      super.run(a)
    }

    override def innerCheckPreconditions: Try[Unit] = checkPreconditions
    override def innerExecute(a: A): Try[S] = execute(a)
    override def innerRollback(): Try[Unit] = rollback()

    override protected def checkPreconditions: Try[Unit] = {
      List(self, other).map(_.innerCheckPreconditions).collectResults.map(_ => ())
    }

    override protected def execute(a: A): Try[S] = {
      for {
        t <- self.innerExecute(a)
        _ = pastLeft = true
        s <- other.innerExecute(t)
      } yield s
    }

    override protected def rollback(): Try[Unit] = {
      (if (pastLeft) List(other, self) else List(self))
        .map(_.innerRollback())
        .collectResults
        .map(_ => ())
    }
  }
}

object Action {
  def apply[A, T](precondition: () => Try[Unit] = () => Success(()),
                  action: A => Try[T],
                  undo: () => Try[Unit] = () => Success(())): Action[A, T] = new Action[A, T] {
    override protected def checkPreconditions: Try[Unit] = precondition()
    override protected def execute(a: A): Try[T] = action(a)
    override protected def rollback(): Try[Unit] = undo()
  }
}

trait UnitAction[+T] extends Action[Unit, T] {
  protected def execute(u: Unit): Try[T] = execute()
  protected def execute(): Try[T]
}
