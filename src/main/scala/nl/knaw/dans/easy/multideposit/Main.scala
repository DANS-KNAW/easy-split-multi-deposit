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

import nl.knaw.dans.easy.multideposit.actions._
import nl.knaw.dans.easy.multideposit.{CommandLineOptions => cmd, MultiDepositParser => parser}
import org.slf4j.LoggerFactory
import rx.lang.scala.{Observable, ObservableExtensions}
import rx.schedulers.Schedulers

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object Main {

  implicit val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    log.debug("Starting application.")
    implicit val settings = cmd.parse(args)
    getActionsStream
      .doOnError(e => log.error(e.getMessage))
      .doOnError(e => log.debug(e.getMessage, e))
      .doOnCompleted { log.info("Finished successfully!") }
      .doOnTerminate {
        // close LDAP at the end of the main
        log.debug("closing ldap")
        settings.ldap.close()
      }
      .subscribe

    Schedulers.shutdown()
  }

  def getActionsStream(implicit settings: Settings): Observable[Action] = {
    parser.parse(multiDepositInstructionsFile(settings))
      .flatMap(_.toObservable)
      .getActions
      .checkActionPreconditions
      .runActions
  }

  // Extension methods are used here to do composition
  // (see http://reactivex.io/rxscala/comparison.html)
  implicit class ActionExtensionMethods(val actions: Observable[Action]) extends AnyVal {
    def checkActionPreconditions = Main.checkActionPreconditions(actions)

    def runActions = Main.runActions(actions)
  }

  implicit class DatasetsExtensionMethods(val datasets: Observable[(DatasetID, Dataset)]) extends AnyVal {
    def getActions(implicit s: Settings) = Main.getActions(datasets)
  }

  def getActions(dss: Observable[(DatasetID, Dataset)])(implicit settings: Settings): Observable[Action] = {
    log.info("Compiling list of actions to perform ...")

    // object for grouping the actions and looking for failures
    case class TryAndFailure(total: List[Try[Action]] = Nil, fails: List[Try[Action]] = Nil) {
      def +=(action: Try[Action]) = {
        TryAndFailure(total :+ action, if (action.isFailure) fails :+ action else fails)
      }
      def toObservable = {
        if (fails.isEmpty) total.toObservable.map(_.get)
        else Observable.error(new Exception(
          generateErrorReport("Errors in Multi-Deposit Instructions file:", fails)))
      }
    }

    // object for collecting both the datasets and the actions
    case class Result(dss: List[(DatasetID, Dataset)] = Nil, actions: Observable[Action] = Observable.empty) {
      def +=(ds: (DatasetID, Dataset), action: Observable[Action]) = Result(dss :+ ds, actions ++ action)
    }

    def getActionsPerEntry(entry: (DatasetID, Dataset)) = (entry, getDatasetActions(entry))

    def addToResult(res: Result, daa: ((DatasetID, Dataset), Observable[Try[Action]])): Result = {
      res += (daa._1, daa._2.foldLeft(TryAndFailure())(_ += _).flatMap(_.toObservable))
    }

    def addCreateSpringfieldActions(result: Result): Observable[Action] = {
      result.actions :+ CreateSpringfieldActions(-1, result.dss.to[ListBuffer])
    }

    dss.map(getActionsPerEntry)
      .foldLeft(Result())(addToResult)
      .flatMap(addCreateSpringfieldActions) // because of foldLeft above, this function is only executed once
  }

  def getDatasetActions(entry: (DatasetID, Dataset))(implicit settings: Settings): Observable[Try[Action]] = {
    val datasetID = entry._1
    val dataset = entry._2
    val row = dataset("ROW").head.toInt // first occurence of dataset, assuming it is not empty

    log.debug(s"Getting actions for dataset $datasetID ...")

    Observable.just(
      CreateOutputDepositDir(row, datasetID),
      AddBagToDeposit(row, datasetID),
      AddDatasetMetadataToDeposit(row, entry),
      AddFileMetadataToDeposit(row, entry),
      AddPropertiesToDeposit(row, entry)
    ).map(Success(_)) ++ getFileActions(dataset, extractFileParameters(dataset))
  }

  def getFileActions(dataset: Dataset, fpss: Observable[FileParameters])(implicit settings: Settings): Observable[Try[Action]] = {
    def getActions(fps: FileParameters): Try[Observable[Action]] = {
//      fps match {
//        // TODO add actions here if needed, remove this function otherwise
//        case FileParameters(Some(row), p1, p2, p3, p4, p5) => Failure(ActionException(row,
//          s"Invalid combination of file parameters: FILE_SIP = $p1, FILE_DATASET = $p2, " +
//            s"FILE_STORAGE_SERVICE = $p3, FILE_STORAGE_PATH = $p4, FILE_AUDIO_VIDEO = $p5"))
//        case _ => Failure(new RuntimeException("*** Programming error: row without row number ***"))
//      }
      Success(Observable.empty)
    }

    // TODO in case getActions(FileParameters): Try[Observable[Action]] gets removed,
    // a lot of other complexity can be removed as well!

    fpss.flatMap(getActions(_).map(as => as.map(Success(_))).onError(exc => Observable.just(Failure(exc))))
      .merge(fpss.collect[Try[Action]] {
        case FileParameters(Some(row), Some(fileMd), _, _, _, Some(isThisAudioVideo))
          if isThisAudioVideo matches "(?i)yes" => Success(CopyToSpringfieldInbox(row, fileMd))
      })
  }

  /**
    * Checks the preconditions for each action it receives. If the preconditions for all
    * actions are met, the actions will be returned within an `Observable[Action]` stream.
    * If any of the preconditions fails, an error report will be generated and returned as
    * an Exception in the stream.
    *
    * @param actions the actions to be checked on preconditions
    * @return A stream of all actions when all preconditions are met; an exception if any
    *         of the preconditions fails.
    */
  def checkActionPreconditions(actions: Observable[Action]): Observable[Action] = {
    case class ActionAndResult(actions: List[Action] = Nil, fails: List[Try[Unit]] = Nil) {
      def +=(action: Action) = {
        ActionAndResult(actions :+ action, {
          val check = action.checkPreconditions

          if (check.isFailure) fails :+ check else fails
        })
      }
      def toObservable = {
        if (fails.isEmpty) actions.toObservable
        else Observable.error(new Exception(generateErrorReport("Precondition failures:", fails,
          "Due to these errors in the preconditions, nothing was done.")))
      }
    }

    actions
      .doOnNext(action => log.info(s"Checking preconditions of ${action.getClass.getSimpleName} ..."))
      .foldLeft(ActionAndResult())((acc, cur) => acc += cur)
      .flatMap(_.toObservable)
  }

  /**
    * Executes all actions in the input `List`. Once an action is completed,
    * it is returned via an `Observable` stream. If an action fails, the action is
    * returned, followed by an error. This also causes the stream to end and not
    * proceed executing any later actions. Also, when an action fails, all previous
    * actions will roll back.
    *
    * @param actions a list of actions to be performed.
    * @return a stream containing the actions if successfull and an error on failure.
    */
  def runActions(actions: Observable[Action]): Observable[Action] = {
    // you can't do this within the Observable sequence, since the stack would
    // not be available when we need it in the .doOnError(e => ...)
    val stack = mutable.Stack[Action]()

    actions
      .doOnNext(action => log.info(s"Executing action of ${action.getClass.getSimpleName} ..."))
      .flatMap(action => action.run()
        .map(_ => Observable.just(action))
        .onError(error => Observable.just(action) ++ Observable.error(error)))
      .doOnNext(stack.push(_))
      .doOnError(_ => {
        if (stack.isEmpty)
          log.error("An error occurred ...")
        else
          log.error("An error occurred. Rolling back actions ...")
        stack.foreach(_.rollback())
      })
  }
}
