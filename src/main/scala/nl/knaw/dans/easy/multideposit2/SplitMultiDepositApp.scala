package nl.knaw.dans.easy.multideposit2

import java.nio.file.Path
import java.util.Locale

import nl.knaw.dans.easy.multideposit2.PathExplorer.PathExplorers
import nl.knaw.dans.easy.multideposit2.actions.{ AddBagToDeposit, CreateDirectories }
import nl.knaw.dans.easy.multideposit2.model.Deposit
import nl.knaw.dans.easy.multideposit2.parser.MultiDepositParser
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

trait SplitMultiDepositApp extends DebugEnhancedLogging {
  this: PathExplorers
    with CreateDirectories
    with AddBagToDeposit =>

  def validate(): Try[Unit] = {
    for {
      _ <- Try { Locale.setDefault(Locale.US) }
      deposits <- MultiDepositParser.parse(multiDepositDir)
    } yield ???
  }

  def convert(): Try[Unit] = {
    for {
      _ <- Try { Locale.setDefault(Locale.US) }
      deposits <- MultiDepositParser.parse(multiDepositDir)
      _ <- deposits.mapUntilFailure(convert)
    } yield ()
  }

  private def convert(deposit: Deposit): Try[Unit] = {
    logger.debug(s"convert ${ deposit.depositId }")
    for {
      _ <- createDepositDirectories(deposit.depositId)
      _ <- addBagToDeposit(deposit.depositId, deposit.profile.created)
      _ <- createMetadataDirectory(deposit.depositId)
    } yield ()
  }
}

object SplitMultiDepositApp {
  def apply(smd: Path, sd: Path, od: Path): SplitMultiDepositApp = {
    new SplitMultiDepositApp with PathExplorers with CreateDirectories with AddBagToDeposit {
      def multiDepositDir: Path = smd

      def stagingDir: Path = sd

      def outputDepositDir: Path = od
    }
  }
}
