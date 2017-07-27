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
package nl.knaw.dans.easy.multideposit.actions

import javax.naming.directory.{ Attributes, BasicAttribute, BasicAttributes }

import nl.knaw.dans.easy.multideposit.{ Ldap, Settings, UnitSpec, _ }
import org.scalamock.scalatest.MockFactory

import scala.util.{ Failure, Success }

class RetrieveDatamanagerActionSpec extends UnitSpec with MockFactory {

  val ldapMock: Ldap = mock[Ldap]
  implicit val settings = Settings(
    datamanager = "dm",
    ldap = ldapMock
  )

  private val correctDatamanagerAttrs = createDatamanagerAttributes()

  /**
   * Default creates correct BasicAttributes
   */
  def createDatamanagerAttributes(state: String = "ACTIVE",
                                  roles: Seq[String] = Seq("USER", "ARCHIVIST"),
                                  mail: String = "dm@test.org"): BasicAttributes = {
    new BasicAttributes() {
      put("dansState", state)
      put(new BasicAttribute("easyRoles") {
        roles.foreach(add)
      })
      put("mail", mail)
    }
  }

  def mockLdapForDatamanager(attrs: Attributes): Unit = {
    (ldapMock.query(_: String)(_: Attributes => Attributes)) expects("dm", *) once() returning Success(Seq(attrs))
  }

  "checkPreconditions" should "succeed if the datamanager email can be retrieved" in {
    mockLdapForDatamanager(correctDatamanagerAttrs)

    RetrieveDatamanagerAction().checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if ldap does not return anything for the datamanager" in {
    (ldapMock.query(_: String)(_: Attributes => Attributes)) expects("dm", *) returning Success(Seq.empty)

    inside(RetrieveDatamanagerAction().checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include("""The datamanager "dm" is unknown""")
    }
  }

  it should "fail when the datamanager is not an active user" in {
    val nonActiveDatamanagerAttrs = createDatamanagerAttributes(state = "BLOCKED")
    mockLdapForDatamanager(nonActiveDatamanagerAttrs)

    inside(RetrieveDatamanagerAction().checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include("not an active user")
    }
  }

  it should "fail when the datamanager is not an achivist" in {
    val nonArchivistDatamanagerAttrs = createDatamanagerAttributes(roles = Seq("USER"))
    mockLdapForDatamanager(nonArchivistDatamanagerAttrs)

    inside(RetrieveDatamanagerAction().checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include("is not an archivist")
    }
  }

  it should "fail when the datamanager has no email" in {
    val nonEmailDatamanagerAttrs = createDatamanagerAttributes(mail = "")
    mockLdapForDatamanager(nonEmailDatamanagerAttrs)

    inside(RetrieveDatamanagerAction().checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include("does not have an email address")
    }
  }

  "execute" should "generate the properties file" in {
    mockLdapForDatamanager(correctDatamanagerAttrs)

    inside(RetrieveDatamanagerAction().execute()) {
      case Success(mail) => mail shouldBe "dm@test.org"
    }
  }

  it should "only compute the datamanager email once, eventhough some methods were called twice" in {
    // this call makes sure ldap is only called once
    mockLdapForDatamanager(correctDatamanagerAttrs)
    val action = RetrieveDatamanagerAction()

    action.checkPreconditions shouldBe a[Success[_]]
    action.execute() shouldBe a[Success[_]]
    action.checkPreconditions shouldBe a[Success[_]]
    action.execute() shouldBe a[Success[_]]
  }
}
