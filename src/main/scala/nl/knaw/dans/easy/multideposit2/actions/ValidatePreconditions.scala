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
package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Path }

import nl.knaw.dans.easy.multideposit2.PathExplorer.StagingPathExplorer
import nl.knaw.dans.easy.multideposit2.model.DepositId
import nl.knaw.dans.easy.multideposit2.model.Deposit
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

trait ValidatePreconditions extends DebugEnhancedLogging {
  this: StagingPathExplorer =>

  def validateDeposit(deposit: Deposit): Try[Unit] = {
    val id = deposit.depositId
    for {
      _ <- checkDirectoriesDoNotExist(id)(stagingDir(id), stagingBagDir(id), stagingBagMetadataDir(id))
      _ <- checkSpringFieldDepositHasAVformat(deposit)
    } yield ()
  }

  def checkDirectoriesDoNotExist(depositId: DepositId)(paths: Path*): Try[Unit] = {
    logger.debug(s"check directories don't exist yet: ${ paths.mkString("[", ", ", "]") }")

    paths.find(Files.exists(_))
      .map(file => Failure(ActionException(s"The deposit for dataset $depositId already exists in $file.")))
      .getOrElse(Success(()))
  }

  def checkSpringFieldDepositHasAVformat(deposit: Deposit): Try[Unit] = {
    logger.debug("check that a Springfield deposit has an A/V format")

    deposit.springfield match {
      case None => Success(())
      case Some(_) => deposit.metadata.formats
        .find(s => s.startsWith("audio/") || s.startsWith("video/"))
        .map(_ => Success(()))
        .getOrElse(Failure(ActionException(
          "No audio/video format found for this column: [DC_FORMAT]\n" +
            "cause: this column should contain at least one " +
            "audio/ or video/ value because SF columns are present")))
    }
  }
}
