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
package nl.knaw.dans.easy.multideposit2

import java.nio.file.{ Files, Paths }
import javax.naming.Context
import javax.naming.ldap.InitialLdapContext

import scala.util.Failure
import nl.knaw.dans.easy.multideposit.FileExtensions

object Test extends App {

  val configuration = Configuration(Paths.get("src/main/assembly/dist"))
  val smd = Paths.get("src/test/resources/allfields/input")
  val sd = Paths.get("target/stagingDir")
  val od = Paths.get("target/outputDir")

  sd.deleteDirectory()
  od.deleteDirectory()

  Files.createDirectories(sd)
  Files.createDirectories(od)

  val ldap = {
    val env = new java.util.Hashtable[String, String]
    env.put(Context.PROVIDER_URL, configuration.properties.getString("auth.ldap.url"))
    env.put(Context.SECURITY_AUTHENTICATION, "simple")
    env.put(Context.SECURITY_PRINCIPAL, configuration.properties.getString("auth.ldap.user"))
    env.put(Context.SECURITY_CREDENTIALS, configuration.properties.getString("auth.ldap.password"))
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")

    Ldap(new InitialLdapContext(env, null))
  }
  val depositPermissions = DepositPermissions(configuration.properties.getString("deposit.permissions.access"), configuration.properties.getString("deposit.permissions.group"))

  val app = SplitMultiDepositApp(smd, sd, od, configuration.formats, "archie001", ldap, depositPermissions)
  app.convert()
    .recoverWith { case e => e.printStackTrace(); Failure(e) }
    .foreach(_ => println("success"))
}
