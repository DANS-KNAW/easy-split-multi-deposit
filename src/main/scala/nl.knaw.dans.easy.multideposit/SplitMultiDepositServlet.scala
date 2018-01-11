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

import java.nio.file.{ Files, Path, Paths }
import java.util.UUID

import net.lingala.zip4j.core.ZipFile
import net.lingala.zip4j.exception.ZipException
import nl.knaw.dans.easy.multideposit.PathExplorer.PathExplorers
import nl.knaw.dans.easy.multideposit.actions.{ ActionException, InvalidDatamanagerException }
import nl.knaw.dans.easy.multideposit.parser.{ EmptyInstructionsFileException, ParserFailedException }
import nl.knaw.dans.lib.error.TryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.scalatra._

import scala.util.Try

class SplitMultiDepositServlet(app: SplitMultiDepositApp, configuration: Configuration) extends ScalatraServlet with DebugEnhancedLogging {
  logger.info("PID Generator Servlet running...")

  private val smdInbox = Paths.get(configuration.properties.getString("inbox-dir"))
  private val stagingDir = Paths.get(configuration.properties.getString("staging-dir"))
  private val ingestFlowInbox = Paths.get(configuration.properties.getString("ingest-flow.inbox"))

  get("/") {
    contentType = "text/plain"
    Ok(s"Split Multi Deposit service running (v${ configuration.version })")
  }

  // PUT /create?datamanager={...}
  put("/ingest") {
    params.get("datamanager")
      .map(datamanager => {
        val result = for {
          mdDir <- extractZip()
          paths = new PathExplorers(mdDir, stagingDir, ingestFlowInbox)
          _ <- app.convert(paths, datamanager)
          _ = mdDir.deleteDirectory()
        } yield Created("Finished successfully! The output is forwarded to easy-ingest-flow")

        result.doIfFailure {
          case e: ParserFailedException => logger.error(e.getMessage)
          case e: InvalidDatamanagerException => logger.error(e.getMessage)
          case e: EmptyInstructionsFileException => logger.error(e.getMessage)
          case e => logger.error(e.getMessage, e)
        }.getOrRecover {
          case e: ParserFailedException => NotAcceptable(e.getMessage)
          case e: InvalidDatamanagerException => NotAcceptable(e.getMessage)
          case e: EmptyInstructionsFileException => NotAcceptable(e.getMessage)
          case e: ActionException => InternalServerError(s"Error while ingesting a multideposit: ${ e.getMessage }")
          case e: ZipException => InternalServerError(s"Not able to unzip the input: ${ e.getMessage }")
          case e => InternalServerError(s"Error while ingesting a multideposit: ${ e.getMessage }")
        }
      })
      .getOrElse(BadRequest("No datamanager specified"))
  }

  // PUT /validate?datamanager={...}
  put("/validate") {
    params.get("datamanager")
      .map(datamanager => {
        val result = for {
          mdDir <- extractZip()
          paths = new PathExplorers(mdDir, stagingDir, ingestFlowInbox)
          deposits <- app.validate(paths, datamanager)
          _ = mdDir.deleteDirectory()
          // TODO return some kind of json representation of the deposits instead?
        } yield Ok("Finished successfully! Everything looks good.")

        result.getOrRecover {
          case e: ParserFailedException => NotAcceptable(e.getMessage)
          case e: InvalidDatamanagerException => NotAcceptable(e.getMessage)
          case e: EmptyInstructionsFileException => NotAcceptable(e.getMessage)
          case e: ActionException => InternalServerError(s"Error while ingesting a multideposit: ${ e.getMessage }")
          case e: ZipException => InternalServerError(s"Not able to unzip the input: ${ e.getMessage }")
          case e => InternalServerError(s"Error while ingesting a multideposit: ${ e.getMessage }")
        }
      })
      .getOrElse(BadRequest("No datamanager specified"))
  }

  private def extractZip(): Try[Path] = Try {
    val mdDir = Files.createTempFile(smdInbox, "multideposit-zip-", "")
    val zip = smdInbox.resolve(s"input-${ UUID.randomUUID() }.zip")

    Files.deleteIfExists(mdDir)
    Files.createDirectory(mdDir)

    Files.copy(request.inputStream, zip)
    new ZipFile(zip.toFile) {
      setFileNameCharset(encoding.name)
    }.extractAll(mdDir.toAbsolutePath.toString)
    Files.delete(zip)

    mdDir
  }
}
