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

import java.util.Locale

import better.files.File
import cats.data.{ NonEmptyChain, ValidatedNec }
import cats.instances.either._
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.traverse._
import javax.naming.Context
import javax.naming.ldap.InitialLdapContext
import nl.knaw.dans.easy.multideposit.PathExplorer._
import nl.knaw.dans.easy.multideposit.actions.{ RetrieveDatamanager, _ }
import nl.knaw.dans.easy.multideposit.model.{ Datamanager, DatamanagerEmailaddress, Deposit }
import nl.knaw.dans.easy.multideposit.parser.MultiDepositParser
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.language.postfixOps

class SplitMultiDepositApp(formats: Set[String], userLicenses: Set[String], ldap: Ldap, ffprobe: FfprobeRunner, permissions: DepositPermissions) extends AutoCloseable with DebugEnhancedLogging {
  private val datamanager = new RetrieveDatamanager(ldap)
  private val createMultiDeposit = new CreateMultiDeposit(formats, ldap, ffprobe, permissions)

  override def close(): Unit = ldap.close()

  def validate(paths: PathExplorers, datamanagerId: Datamanager): Either[NonEmptyChain[Throwable], Seq[Deposit]] = {
    implicit val input: InputPathExplorer = paths
    implicit val staging: StagingPathExplorer = paths
    implicit val output: OutputPathExplorer = paths

    type Validated[T] = ValidatedNec[Throwable, T]

    for {
      _ <- Locale.setDefault(Locale.US).asRight[NonEmptyChain[Throwable]]
      deposits <- MultiDepositParser.parse(input.multiDepositDir, userLicenses).leftMap(NonEmptyChain.one)
      _ <- deposits.traverse[Validated, Unit](createMultiDeposit.validateDeposit(_).toValidatedNec).toEither
      _ <- datamanager.getDatamanagerEmailaddress(datamanagerId).leftMap(NonEmptyChain.one)
    } yield deposits
  }

  def convert(paths: PathExplorers, datamanagerId: Datamanager): Either[NonEmptyChain[Throwable], Unit] = {
    implicit val input: InputPathExplorer = paths
    implicit val staging: StagingPathExplorer = paths
    implicit val output: OutputPathExplorer = paths

    type X[T] = Either[ConversionFailed, T]

    for {
      _ <- Locale.setDefault(Locale.US).asRight[NonEmptyChain[Throwable]]
      deposits <- MultiDepositParser.parse(input.multiDepositDir, userLicenses).leftMap(e => NonEmptyChain.one(ParseFailed(e.report)))
      dataManagerEmailAddress <- datamanager.getDatamanagerEmailaddress(datamanagerId).leftMap(NonEmptyChain.one)
      _ <- deposits.traverse[X, Unit](createMultiDeposit.convertDeposit(paths, datamanagerId, dataManagerEmailAddress))
        .leftMap(error => {
          deposits.traverse[X, Unit](d => createMultiDeposit.clearDeposit(d.depositId))
            .fold(discardError => NonEmptyChain(
              ActionException("discarding deposits failed after creating deposits failed"),
              error,
              discardError
            ), _ => NonEmptyChain.one(error))
        })
      _ = logger.info("deposits were created successfully")
      _ <- createMultiDeposit.report(deposits).leftMap(NonEmptyChain.one)
      _ = logger.info(s"report generated at ${ paths.reportFile }")
      _ <- deposits.traverse[X, Unit](deposit => createMultiDeposit.moveDepositsToOutputDir(deposit.depositId, deposit.bagId)).leftMap(NonEmptyChain.one)
      _ = logger.info(s"deposits were successfully moved to ${ output.outputDepositDir }")
    } yield ()
  }
}

object SplitMultiDepositApp {
  def apply(configuration: Configuration): SplitMultiDepositApp = {
    val ldap = {
      val env = new java.util.Hashtable[String, String]
      env.put(Context.PROVIDER_URL, configuration.properties.getString("auth.ldap.url"))
      env.put(Context.SECURITY_AUTHENTICATION, "simple")
      env.put(Context.SECURITY_PRINCIPAL, configuration.properties.getString("auth.ldap.user"))
      env.put(Context.SECURITY_CREDENTIALS, configuration.properties.getString("auth.ldap.password"))
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")

      Ldap(new InitialLdapContext(env, null))
    }
    val permissions = DepositPermissions(
      permissions = configuration.properties.getString("deposit.permissions.access"),
      group = configuration.properties.getString("deposit.permissions.group")
    )
    val ffprobe = {
      val ffProbePath = configuration.properties.getString("audio-video.ffprobe")
      require(ffProbePath != null, "Missing configuration for ffprobe")
      val exeFile = File(ffProbePath)
      require(exeFile isRegularFile, s"Ffprobe at $exeFile does not exist or is not a regular file")
      require(exeFile isExecutable, s"Ffprobe at $exeFile is not executable")
      FfprobeRunner(exeFile)
    }

    new SplitMultiDepositApp(configuration.formats, configuration.licenses, ldap, ffprobe, permissions)
  }
}
