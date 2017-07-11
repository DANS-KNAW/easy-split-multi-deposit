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

import nl.knaw.dans.lib.error.{ CompositeException, TraversableTryExtensions }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

/*
  To future developers: this class is a Category. It should satisfy the law of composition.
 */
trait Action[-A, +T] extends DebugEnhancedLogging {
  self =>

  protected[Action] def logPreconditions(): Unit = {}

  protected[Action] def logExecute(): Unit = {}

  protected[Action] def logRollback(): Unit = {}

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
    def recoverPreconditions(t: Throwable) = {
      Failure(PreconditionsFailedException(
        report = generateReport(
          header = "Precondition failures:",
          throwable = t,
          footer = "Due to these errors in the preconditions, nothing was done."),
        cause = t))
    }

    def recoverRun(t: Throwable) = {
      Failure(ActionRunFailedException(
        report = generateReport(
          header = "Errors during processing:",
          throwable = t,
          footer = "The actions that were already performed, were rolled back."),
        cause = t
      ))
    }

    for {
      _ <- innerCheckPreconditions.recoverWith { case NonFatal(e) => recoverPreconditions(e) }
      t <- innerExecute(a).recoverWith {
        case NonFatal(e) =>
          rollback() match {
            case Success(_) => recoverRun(e)
            case Failure(e2) => recoverRun(new CompositeException(e, e2))
          }
      }
    } yield t
  }

  private def generateReport(header: String = "", throwable: Throwable, footer: String = ""): String = {
    def report(throwable: Throwable): Seq[String] = {
      List(throwable)
        .flatMap {
          case es: CompositeException => es.throwables
          case e => Seq(e)
        }
        .map {
          case ActionException(-1, msg, _) => s" - cmd line: $msg"
          case ActionException(row, msg, _) => s" - row $row: $msg"
          case NonFatal(ex) => s" - unexpected error: ${ ex.getMessage }"
        }
    }

    header.toOption.fold("")(_ + "\n") +
      report(throwable).distinct.mkString("\n") +
      footer.toOption.fold("")("\n" + _)
  }

  /**
   * Sequentially composes two `Action`s by running their `execute` methods one after the other,
   * where the input of `other` is the output of this `Action`.
   *
   * Combining two `Action`s means that the composed `Action` will do the following:
   *
   *   - run `checkPreconditions` for this
   *   - run `checkPreconditions` for other
   *   - if any of these fails, terminate running and report the failures
   *   - if all preconditions succeed, continue with:
   *   - run `execute` for this with the input from `run(a: A)` as its input
   *   - on failure of the previous step: call `rollback` on this `Action`, terminate running and return/report the error
   *   - run `execute` for other with the output from `this.execute` as its input
   *   - on failure of the previous step: call `rollback` on other and this `Action` (in this order!); terminate running and return/report the error
   *   - return the output of calling `other.execute` as the result of this composed `Action`
   *
   * @param other the `Action` to combine this `Action` with
   * @tparam S the output type of the second `Action`
   * @return an `Action` that composes these two actions sequentially
   */
  def combine[S](other: Action[T, S]): Action[A, S] = new Action[A, S] {
    private var pastSelf = false

    override def run(a: A): Try[S] = {
      pastSelf = false
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
        _ = pastSelf = true
        s <- other.innerExecute(t)
      } yield s
    }

    override protected def rollback(): Try[Unit] = {
      (if (pastSelf) List(other, self)
       else List(self))
        .map(_.innerRollback())
        .collectResults
        .map(_ => ())
    }
  }

  def withLogMessages(pre: => String, exe: => String, rb: => String): Action[A, T] = new Action[A, T] {

    override def logPreconditions(): Unit = {
      super.logPreconditions()
      logger.info(pre)
    }

    override def logExecute(): Unit = {
      super.logExecute()
      logger.info(exe)
    }

    override def logRollback(): Unit = {
      super.logRollback()
      logger.info(rb)
    }

    override protected def checkPreconditions: Try[Unit] = self.checkPreconditions

    override protected def execute(a: A): Try[T] = self.execute(a)

    override protected def rollback(): Try[Unit] = self.rollback()
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
