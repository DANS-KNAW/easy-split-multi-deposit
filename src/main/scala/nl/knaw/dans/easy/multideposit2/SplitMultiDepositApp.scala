package nl.knaw.dans.easy.multideposit2

import java.nio.file.Path
import java.util.Locale

import nl.knaw.dans.easy.multideposit2.PathExplorer.PathExplorers
import nl.knaw.dans.easy.multideposit2.actions.{ AddBagToDeposit, CreateDirectories }
import nl.knaw.dans.easy.multideposit2.model.Deposit
import nl.knaw.dans.easy.multideposit2.parser.MultiDepositParser

import scala.util.Try

trait SplitMultiDepositApp extends CreateDirectories with AddBagToDeposit {
  this: PathExplorers =>

  def validate(multiDeposit: Path): Try[Unit] = {
    for {
      _ <- Try { Locale.setDefault(Locale.US) }
      deposits <- MultiDepositParser.parse(multiDeposit)
    } yield ???
  }

  def convert(multiDeposit: Path): Try[Unit] = {
    for {
      _ <- Try { Locale.setDefault(Locale.US) }
      deposits <- MultiDepositParser.parse(multiDeposit)
      _ <- deposits.mapUntilFailure(convert)
    } yield ()
  }

  private def convert(deposit: Deposit): Try[Unit] = {
    for {
      _ <- createDepositDirectories(deposit.depositId)
      _ <- addBagToDeposit(deposit.depositId, deposit.profile.created)
      _ <- createMetadataDirectory(deposit.depositId)
    } yield ()
  }
}
