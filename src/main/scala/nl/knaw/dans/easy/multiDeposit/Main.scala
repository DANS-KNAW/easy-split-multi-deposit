package nl.knaw.dans.easy.multiDeposit

import java.io.File

import nl.knaw.dans.easy.multiDeposit.{CommandLineOptions => cmd, MultiDepositParser => parser}
import org.slf4j.LoggerFactory
import rx.lang.scala.Observable
import rx.lang.scala.ObservableExtensions

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object Main {

  implicit val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    log.debug("Starting application.")
    implicit val settings = cmd.parse(args)
    println(settings)
    getActionsStream
      .doOnError(e => log.error(e.getMessage, e))
      .doOnCompleted(() => log.info("Finished successfully!"))
      .subscribe
  }

  def getActionsStream(implicit settings: Settings): Observable[Action] = {
    parser.parse(new File(settings.mdDir, cmd.mdInstructionsFileName))
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
    val tryActions = dss.flatMap(getDatasetActions).toList // TODO add extra actions with :+ ...
    val failures = tryActions.filter(_.isFailure)
    if (failures.isEmpty)
      tryActions.map(_.get).toObservable
    else
      Observable.error(new Exception(generateErrorReport("Errors in Multi-Deposit Instructions file:", failures)))
  }

  def getDatasetActions(entry: (DatasetID, Dataset))(implicit s: Settings): List[Try[Action]] = {
    val datasetID = entry._1
    val dataset = entry._2
    val row = dataset("ROW").head // first occurence of dataset

    log.debug("Getting actions for dataset {} ...", datasetID)

    val fpss = extractFileParametersList(dataset)

    // TODO add prior actions here using ::
    getFileActions(dataset, fpss)
  }

  def getFileActions(d: Dataset, fpss: List[FileParameters])(implicit s: Settings): List[Try[Action]] = {

    def getActions(fps: FileParameters): List[Action] = {
      fps match {
        // TODO add actions here
        case FileParameters(Some(row), p1, p2, p3, p4, p5) => throw ActionException(row,
          s"Invalid combination of file parameters: FILE_SIP = $p1, FILE_DATASET = $p2, " +
            s"FILE_STORAGE_SERVICE = $p3, FILE_STORAGE_PATH = $p4, FILE_AUDIO_VIDEO = $p5")
      }

      Nil
    }

    log.debug("Looking for file instructions ...")

    fpss.flatMap(fps => {
      try getActions(fps).map(Success(_))
      catch {
        case e: ActionException => List(Failure(e))
      }
    })
    // TODO add actions here using :::
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
    log.info("Checking preconditions ...")

    case class ActionAndResult(actions: List[Action] = Nil, result: List[Try[Unit]] = Nil)

    actions
      .foldLeft(ActionAndResult()) {
        case (ActionAndResult(total, fails), t) => ActionAndResult(total :+ t, {
          val check = t.checkPreconditions

          if (check.isFailure) fails :+ check else fails
        })
      }
      .flatMap {
        case ActionAndResult(total, Nil) => total.toObservable
        case ActionAndResult(_, fails) => Observable.error(new Exception(generateErrorReport("Precondition failures:", fails)))
      }
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
    log.info("Executing actions ...")

    // you can't do this within the Observable sequence, since the stack would
    // not be available when we need it in the .doOnError(e => ...)
    val stack = mutable.Stack[Action]()

    actions
      .flatMap(action => action.run()
        .map(_ => Observable.just(action))
        .onError(error => Observable.just(action) ++ Observable.error(error)))
      .doOnNext(stack.push(_))
      .doOnError(_ => {
        log.error("An error occurred. Rolling back actions ...")
        stack.foreach(_.rollback)
      })
  }
}
