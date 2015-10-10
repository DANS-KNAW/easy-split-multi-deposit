package nl.knaw.dans.easy.ps

import java.io.File
import java.text.Normalizer
import org.apache.commons.lang.StringUtils.isBlank
import org.slf4j.LoggerFactory
import rx.lang.scala.Observable

import scala.util.{Failure, Success, Try}

object Main {
  implicit val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    log.debug("Starting application.")
    implicit val settings = CommandLineOptions.parse(args)
    getActionsStream.subscribe(
      x => (),
      e => log.error(e.getMessage),
      () => log.info("Finished successfully!")
    )
  }

  def getActionsStream(implicit settings: Settings): Observable[Action] = {
    SipInstructions.parse(new File(settings.sipDir, CommandLineOptions.sipInstructionsFileName))
      .flatMap(getActions)
      .flatMap(checkActionPreconditions)
      .flatMap(runActions)
  }

  def getActions(ds: Datasets)(implicit s: Settings): Observable[List[Action]] = {
    log.info("Compiling list of actions to perform ...")
    val tryActions = ds.flatMap(getDatasetActions).toList :::
      Success(CreateSpringfieldActions(ds.size.toString, ds)) ::
      Nil
    val failures = tryActions.filter(_.isFailure)
    if (failures.isEmpty)
      Observable.just(tryActions.map(_.get))
    else
      Observable.error(createErrorReporterException("Errors in SIP Instructions file:", failures))
  }

  def getDatasetActions(entry: (DatasetID, Dataset))(implicit s: Settings): List[Try[Action]] = {
    val datasetId = entry._1
    val dataset = entry._2
    val row = entry._2("ROW").head
    log.debug("Getting actions for dataset {} ...", datasetId)
    val calculatedDatasetStoragePath = calculateDatasetStoragePath(dataset)
    val fpss = extractFileParametersList(dataset)
    Success(CreateDatasetIngestDir(row, dataset, fpss)) ::
      Success(AddEmdToDatasetIngestDir(row, dataset)) ::
      Success(AddAmdToDatasetIngestDir(row, datasetId)) ::
      getFileActions(dataset, fpss, calculatedDatasetStoragePath)
  }

  def getFileActions(d: Dataset, fpss: List[FileParameters], calculatedDatasetStoragePath: Option[String])(implicit s: Settings): List[Try[Action]] = {

    def getActions(fps: FileParameters): List[Action] = {
      fps match {
        case (Some(row), Some(fileSip), Some(fileInDataset), None, None, _) =>
          CopyFileToDatasetIngestDir(row, id(d), fileSip, fileInDataset) ::
            Nil
        case (Some(row), Some(fileSip), Some(fileInDataset), Some(storageService), None, _) =>
          CopyFileToStorage(row, storageService, fileSip, calculatedDatasetStoragePath, fileInDataset) ::
            CreateDataFileInstructions(row, Some(fileSip), d("DATASET_ID").head, fileInDataset, storageService, calculatedDatasetStoragePath, fileInDataset) ::
            Nil
        case (Some(row), Some(fileSip), Some(fileInDataset), Some(storageService), Some(fileStoragePath), _) =>
          CopyFileToStorage(row, storageService, fileSip, Some(""), fileStoragePath) ::
            CreateDataFileInstructions(row, Some(fileSip), d("DATASET_ID").head, fileInDataset, storageService, Some(""), fileStoragePath) ::
            Nil
        case (Some(row), None, Some(fileInDataset), Some(storageService), Some(fileStoragePath), _) =>
          CheckExistenceInStorage(row, storageService, fileStoragePath) ::
            CreateDataFileInstructions(row, None, d("DATASET_ID").head, fileInDataset, storageService, Some(""), fileStoragePath) ::
            Nil
        case (Some(row), p1, p2, p3, p4, p5) => throw ActionException(row,
          s"Invalid combination of file parameters: FILE_SIP = $p1, FILE_DATASET = $p2, " +
            s"FILE_STORAGE_SERVICE = $p3, FILE_STORAGE_PATH = $p4, FILE_AUDIO_VIDEO = $p5")
        case _ => throw new RuntimeException("*** Programming error: row without row number ***")
      }
    }

    log.debug("Looking for file instructions ...")
    fpss.flatMap(fps =>
      try getActions(fps).map(Success(_))
      catch { case e: ActionException => Failure(e) :: Nil }
    ) :::
      fpss.collect {
        case (Some(row), Some(fileSip), _, _, _, Some(isThisAudioVideo)) if isThisAudioVideo matches "(?i)yes" =>
          Success(CopyToSpringfieldInbox(row, fileSip))
      }
  }

  def calculateDatasetStoragePath(dataset: Dataset): Option[String] = {
    for {
      a <- getFirstNonBlank(dataset.get("DCX_ORGANIZATION"), Some(List("no-organization")))
      b <- getFirstNonBlank(dataset.get("DCX_CREATOR_DAI"), dataset.get("DCX_CREATOR_SURNAME"), dataset.get("DC_CREATOR"))
      c <- getFirstNonBlank(dataset.get("DC_TITLE"))
    } yield List(a, b, c)
      .map(_.replace('/', '-').replace(' ', '-'))
      .map(Normalizer
        .normalize(_, Normalizer.Form.NFD)
        .replaceAll("[^\\p{ASCII}]", ""))
      .mkString("/")
  }

  def getFirstNonBlank(lists: Option[List[String]]*): Option[String] =
    lists.find(_.isDefined).flatMap(_.get.dropWhile(isBlank).headOption)

  def checkActionPreconditions(actions: List[Action]): Observable[List[Action]] = {
    log.info("Checking preconditions ...")
    val preconditionFailures = actions.map(_.checkPreconditions).filter(_.isFailure)
    if (preconditionFailures.isEmpty)
      Observable.just(actions)
    else
      Observable.error(createErrorReporterException("Precondition failures:", preconditionFailures))
  }

  def runActions(actions: List[Action]): Observable[Action] = {
    log.info("Executing actions ...")
    val stack = collection.mutable.Stack[Action]()
    actions.run()
      .doOnEach(stack.push(_))
      .doOnError(_ => {
        log.error("An error ocurred. Rolling back actions ...")
        stack.foreach(_.rollback())
      })
  }
}
