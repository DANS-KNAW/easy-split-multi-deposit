package nl.knaw.dans.easy.multideposit.actions

import cats.data.{ NonEmptyChain, ValidatedNec }
import cats.instances.either._
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.traverse._
import nl.knaw.dans.easy.multideposit.PathExplorer.{ InputPathExplorer, OutputPathExplorer, PathExplorers, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit.model.{ Datamanager, DatamanagerEmailaddress, Deposit }
import nl.knaw.dans.easy.multideposit.{ ConversionFailed, DepositPermissions, FfprobeRunner, Ldap }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

class CreateMultiDeposit(formats: Set[String],
                         ldap: Ldap,
                         ffprobe: FfprobeRunner,
                         permissions: DepositPermissions) extends DebugEnhancedLogging {

  private val validator = new ValidatePreconditions(ldap, ffprobe)
  private val datamanager = new RetrieveDatamanager(ldap)
  private val createDirs = new CreateDirectories()
  private val createBag = new AddBagToDeposit()
  private val datasetMetadata = new AddDatasetMetadataToDeposit(formats)
  private val fileMetadata = new AddFileMetadataToDeposit()
  private val depositProperties = new AddPropertiesToDeposit()
  private val setPermissions = new SetDepositPermissions(permissions)
  private val reportDatasets = new ReportDatasets()
  private val moveDeposit = new MoveDepositToOutputDir()

  private type ConversionFailedOr[T] = Either[ConversionFailed, T]
  private type ConversionFailedValidated[T] = ValidatedNec[ConversionFailed, T]

  def validateDeposits(deposits: List[Deposit])(implicit staging: StagingPathExplorer): ValidatedNec[ConversionFailed, Unit] = {
    deposits.traverse[ConversionFailedValidated, Unit](validateDeposit).map(_ => ())
  }

  private def validateDeposit(deposit: Deposit)(implicit staging: StagingPathExplorer): ValidatedNec[ConversionFailed, Unit] = {
    validator.validateDeposit(deposit)
      .leftMap(conversionErrorReport)
      .toValidatedNec
  }

  def getDatamanagerEmailaddress(datamanagerId: Datamanager): Either[ConversionFailed, DatamanagerEmailaddress] = {
    datamanager.getDatamanagerEmailaddress(datamanagerId)
      .leftMap(conversionErrorReport)
  }

  def convertDeposits(deposits: List[Deposit],
                      paths: PathExplorers,
                      datamanagerId: Datamanager,
                      dataManagerEmailAddress: DatamanagerEmailaddress)
                     (implicit staging: StagingPathExplorer): Either[NonEmptyChain[ConversionFailed], Unit] = {
    deposits.traverse[ConversionFailedOr, Unit](convertDeposit(paths, datamanagerId, dataManagerEmailAddress))
      .leftMap(error => {
        deposits.traverse[ConversionFailedOr, Unit](d => createDirs.discardDeposit(d.depositId).leftMap(conversionErrorReport))
          .fold(discardError => NonEmptyChain(
            error,
            ConversionFailed("discarding deposits failed after creating deposits failed", Option(discardError))
          ), _ => NonEmptyChain.one(error))
      })
      .map(_ => ())
  }

  private def convertDeposit(paths: PathExplorers,
                             datamanagerId: Datamanager,
                             dataManagerEmailAddress: DatamanagerEmailaddress)
                            (deposit: Deposit): Either[ConversionFailed, Unit] = {
    val depositId = deposit.depositId
    implicit val input: InputPathExplorer = paths
    implicit val staging: StagingPathExplorer = paths
    implicit val output: OutputPathExplorer = paths

    logger.info(s"convert ${ depositId }")

    val result = for {
      _ <- validator.validateDeposit(deposit)
      _ <- createDirs.createDepositDirectories(depositId)
      _ <- createBag.addBagToDeposit(depositId, deposit.profile.created, deposit.baseUUID)
      _ <- createDirs.createMetadataDirectory(depositId)
      _ <- datasetMetadata.addDatasetMetadata(deposit)
      _ <- fileMetadata.addFileMetadata(depositId, deposit.files)
      _ <- depositProperties.addDepositProperties(deposit, datamanagerId, dataManagerEmailAddress)
      _ <- setPermissions.setDepositPermissions(depositId)
      _ = logger.info(s"deposit was created successfully for $depositId with bagId ${ deposit.bagId }")
    } yield ()

    result.leftMap(conversionErrorReport)
  }

  def report(deposits: List[Deposit])(implicit output: OutputPathExplorer): Either[ConversionFailed, Unit] = {
    reportDatasets.report(deposits)
      .leftMap(conversionErrorReport)
  }

  def moveDepositsToOutputDir(deposits: List[Deposit])(implicit staging: StagingPathExplorer, output: OutputPathExplorer): Either[ConversionFailed, Unit] = {
    deposits.traverse[ConversionFailedOr, Unit](deposit => moveDeposit.moveDepositsToOutputDir(deposit.depositId, deposit.bagId)
      .leftMap(conversionErrorReport))
      .map(_ => ())
  }

  private def conversionErrorReport(error: CreateDepositError): ConversionFailed = {
    error match {
      case ActionException(msg, cause) => ConversionFailed(msg, Option(cause))
      case InvalidDatamanagerException(msg) => ConversionFailed(msg)
      case e: InvalidInputException => ConversionFailed(e.getMessage)
      case e: FfprobeErrorException => ConversionFailed(e.getMessage)
    }
  }
}
