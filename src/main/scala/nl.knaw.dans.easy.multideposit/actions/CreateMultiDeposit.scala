package nl.knaw.dans.easy.multideposit.actions

import cats.syntax.either._
import nl.knaw.dans.easy.multideposit.PathExplorer.{ InputPathExplorer, OutputPathExplorer, PathExplorers, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit.model.{ BagId, Datamanager, DatamanagerEmailaddress, Deposit, DepositId }
import nl.knaw.dans.easy.multideposit.{ ConversionFailed, DepositPermissions, FfprobeRunner, Ldap }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

class CreateMultiDeposit(formats: Set[String],
                         ldap: Ldap,
                         ffprobe: FfprobeRunner,
                         permissions: DepositPermissions) extends DebugEnhancedLogging {

  private val validator = new ValidatePreconditions(ldap, ffprobe)
  private val createDirs = new CreateDirectories()
  private val createBag = new AddBagToDeposit()
  private val datasetMetadata = new AddDatasetMetadataToDeposit(formats)
  private val fileMetadata = new AddFileMetadataToDeposit()
  private val depositProperties = new AddPropertiesToDeposit()
  private val setPermissions = new SetDepositPermissions(permissions)
  private val reportDatasets = new ReportDatasets()
  private val moveDeposit = new MoveDepositToOutputDir()

  def validateDeposit(deposit: Deposit)(implicit staging: StagingPathExplorer): Either[ConversionFailed, Unit] = {
    validator.validateDeposit(deposit)
      .leftMap(conversionErrorReport)
  }

  def convertDeposit(paths: PathExplorers, datamanagerId: Datamanager, dataManagerEmailAddress: DatamanagerEmailaddress)(deposit: Deposit): Either[ConversionFailed, Unit] = {
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

  def clearDeposit(depositId: DepositId)(implicit staging: StagingPathExplorer): Either[ConversionFailed, Unit] = {
    createDirs.discardDeposit(depositId)
      .leftMap(conversionErrorReport)
  }

  def report(deposits: Seq[Deposit])(implicit output: OutputPathExplorer): Either[ConversionFailed, Unit] = {
    reportDatasets.report(deposits)
      .leftMap(conversionErrorReport)
  }

  def moveDepositsToOutputDir(depositId: DepositId, bagId: BagId)(implicit staging: StagingPathExplorer, output: OutputPathExplorer): Either[ConversionFailed, Unit] = {
    moveDeposit.moveDepositsToOutputDir(depositId, bagId)
      .leftMap(conversionErrorReport)
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
