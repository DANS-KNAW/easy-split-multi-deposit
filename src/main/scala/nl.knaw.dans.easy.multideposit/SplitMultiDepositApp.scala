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
import cats.data.NonEmptyChain
import cats.syntax.either._
import javax.naming.Context
import javax.naming.ldap.InitialLdapContext
import nl.knaw.dans.easy.multideposit.PathExplorer._
import nl.knaw.dans.easy.multideposit.actions.CreateMultiDeposit
import nl.knaw.dans.easy.multideposit.model.Datamanager
import nl.knaw.dans.easy.multideposit.parser.MultiDepositParser
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

class SplitMultiDepositApp(formats: Set[String], userLicenses: Set[String], ldap: Ldap, ffprobe: FfprobeRunner, permissions: DepositPermissions, dansDoiPrefix: String) extends AutoCloseable with DebugEnhancedLogging {
  private val createMultiDeposit = new CreateMultiDeposit(formats, ldap, ffprobe, permissions, dansDoiPrefix)

  override def close(): Unit = ldap.close()

  def validate(paths: PathExplorers, datamanagerId: Datamanager): Either[NonEmptyChain[SmdError], Unit] = {
    implicit val input: InputPathExplorer = paths
    implicit val staging: StagingPathExplorer = paths

    Locale.setDefault(Locale.US)

    for {
      deposits <- MultiDepositParser.parse(input.multiDepositDir, userLicenses).toEitherNec
      _ <- createMultiDeposit.validateDeposits(deposits).toEither
      _ <- createMultiDeposit.getDatamanagerEmailaddress(datamanagerId).toEitherNec
    } yield ()
  }

  def convert(paths: PathExplorers, datamanagerId: Datamanager): Either[NonEmptyChain[SmdError], Unit] = {
    implicit val input: InputPathExplorer = paths
    implicit val staging: StagingPathExplorer = paths
    implicit val output: OutputPathExplorer = paths

    Locale.setDefault(Locale.US)

    for {
      deposits <- MultiDepositParser.parse(input.multiDepositDir, userLicenses).toEitherNec
      dataManagerEmailAddress <- createMultiDeposit.getDatamanagerEmailaddress(datamanagerId).toEitherNec
      _ <- createMultiDeposit.convertDeposits(deposits, paths, datamanagerId, dataManagerEmailAddress)
      _ = logger.info("deposits were created successfully")
      _ <- createMultiDeposit.report(deposits).toEitherNec
      _ = logger.info(s"report generated at ${ paths.reportFile }")
      _ <- createMultiDeposit.moveDepositsToOutputDir(deposits).toEitherNec
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
      require(exeFile.isRegularFile, s"Ffprobe at $exeFile does not exist or is not a regular file")
      require(exeFile.isExecutable, s"Ffprobe at $exeFile is not executable")
      FfprobeRunner(exeFile)
    }
    val dansDoiPrefix = configuration.properties.getString("dans.doi.prefix")

    new SplitMultiDepositApp(configuration.formats, configuration.licenses, ldap, ffprobe, permissions, dansDoiPrefix)
  }
}
