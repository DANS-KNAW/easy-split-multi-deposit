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

import java.io.File

import nl.knaw.dans.easy.multideposit.actions._
import nl.knaw.dans.easy.multideposit.{CommandLineOptions => cmd, MultiDepositParser => parser}
import org.slf4j.LoggerFactory
import rx.lang.scala.{Observable, ObservableExtensions}
import rx.schedulers.Schedulers

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object Main {

  implicit val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    log.debug("Starting application.")
    implicit val settings = cmd.parse(args)
    getActionsStream
      .doOnError(e => log.error(e.getMessage, e))
      .doOnCompleted { log.info("Finished successfully!") }
      .subscribe

    Schedulers.shutdown()
  }

  def getActionsStream(implicit settings: Settings): Observable[Action] = {
    parser.parse(new File(settings.multidepositDir, cmd.mdInstructionsFileName))
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

  implicit class DatasetsExtensionMethods(val datasets: Observable[Datasets]) extends AnyVal {
    def getActions(implicit s: Settings) = datasets.flatMap(Main.getActions)
  }

  def getActions(dss: Datasets)(implicit s: Settings): Observable[Action] = {
    log.info("Compiling list of actions to perform ...")
    val tryActions = dss.flatMap(getDatasetActions).toList :+
      Success(CreateSpringfieldActions(-1, dss)) // SpringfieldAction runs on multiple rows, so -1 here

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

    tryActions.toObservable
      .foldLeft(TryAndFailure())(_ += _)
      .flatMap(_.toObservable)
  }

  def getDatasetActions(entry: (DatasetID, Dataset))(implicit s: Settings): List[Try[Action]] = {
    val datasetID = entry._1
    val dataset = entry._2
    val row = dataset("ROW").head.toInt // first occurence of dataset, assuming it is not empty

    log.debug("Getting actions for dataset {} ...", datasetID)

    val fpss = extractFileParametersList(dataset)

    Success(CreateOutputDepositDir(row, datasetID)) ::
    Success(AddBagToDeposit(row, datasetID)) ::
    // TODO add more here: AddMetadataToDepositAction and AddPropertiesToDepositAction
    Success(CreateMetadata(row)) ::
      getFileActions(dataset, fpss)
  }

  def getFileActions(d: Dataset, fpss: List[FileParameters])(implicit s: Settings): List[Try[Action]] = {

    def getActions(fps: FileParameters): List[Action] = {
//      fps match {
//        // TODO add actions here if needed
//        case FileParameters(Some(row), p1, p2, p3, p4, p5) => throw ActionException(row,
//          s"Invalid combination of file parameters: FILE_SIP = $p1, FILE_DATASET = $p2, " +
//            s"FILE_STORAGE_SERVICE = $p3, FILE_STORAGE_PATH = $p4, FILE_AUDIO_VIDEO = $p5")
//      }

      Nil // TODO remove later
    }

    log.debug("Looking for file instructions ...")

    fpss.flatMap(fps => {
      Try {
        getActions(fps).map(Success(_))
      } onError {
        case e: ActionException => List(Failure(e))
      }
    }) :::
      fpss.collect {
        case FileParameters(Some(row), Some(fileMd), _, _, _, Some(isThisAudioVideo)) if isThisAudioVideo matches "(?i)yes" =>
          Success(CopyToSpringfieldInbox(row, fileMd))
      }
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
        else Observable.error(new Exception(generateErrorReport("Precondition failures:", fails)))
      }
    }

    actions
      .doOnNext(action => log.info(s"Checking preconditions of ${action.getClass.getSimpleName} ..."))
      .foldLeft(ActionAndResult())(_ += _)
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
        log.error("An error occurred. Rolling back actions ...")
        stack.foreach(_.rollback())
      })
  }
}
