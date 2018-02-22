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

import java.nio.file.Paths

import nl.knaw.dans.easy.multideposit.PathExplorer.PathExplorers
import nl.knaw.dans.easy.multideposit.actions.{ InvalidDatamanagerException, InvalidInputException }
import nl.knaw.dans.easy.multideposit.parser.{ EmptyInstructionsFileException, ParserFailedException }
import nl.knaw.dans.lib.error.TryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import resource.managed

import scala.language.reflectiveCalls
import scala.util.Try
import scala.util.control.NonFatal

object Command extends App with DebugEnhancedLogging {

  type FeedBackMessage = String

  val configuration = Configuration(Paths.get(System.getProperty("app.home")))
  val commandLine: CommandLineOptions = new CommandLineOptions(args, configuration.version)
  val app = SplitMultiDepositApp(configuration)

  managed(app)
    .acquireAndGet(runSubcommand)
    .doIfSuccess(msg => println(s"OK: $msg"))
    .doIfFailure {
      case ParserFailedException(_, _) |
           EmptyInstructionsFileException(_) |
           InvalidDatamanagerException(_) |
           InvalidInputException(_, _) => // do nothing
      case e => logger.error(e.getMessage, e)
    }
    .doIfFailure { case NonFatal(e) => println(s"FAILED: ${ e.getMessage }") }

  private def runSubcommand(app: SplitMultiDepositApp): Try[FeedBackMessage] = {
    lazy val defaultStagingDir = Paths.get(configuration.properties.getString("staging-dir"))

    val md = commandLine.multiDepositDir()
    val sd = commandLine.stagingDir.getOrElse(defaultStagingDir)
    val od = commandLine.outputDepositDir()
    val dm = commandLine.datamanager()

    if (commandLine.validateOnly())
      app.validate(new PathExplorers(md, sd, od), dm)
        .map(_ => "Finished successfully! Everything looks good.")
    else
      app.convert(new PathExplorers(md, sd, od), dm)
        .map(_ => s"Finished successfully! The output can be found in $od.")
  }
}
