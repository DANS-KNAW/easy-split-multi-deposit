/*
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
import nl.knaw.dans.easy.multideposit.{ Ldap, TestSupportFixture }
import org.scalamock.scalatest.MockFactory

class RetrieveDatamanagerSpec extends TestSupportFixture with MockFactory {

  private val ldapMock: Ldap = mock[Ldap]
  private val datamanagerId = "dm"
  private val action = new RetrieveDatamanager(ldapMock)

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

  def mockLdapForDatamanager(attrs: Seq[Attributes]): Unit = {
    (ldapMock.query(_: String)(_: Attributes => Attributes)) expects("dm", *) once() returning Right(attrs)
  }

  "getDatamanagerEmailaddress" should "succeed if the datamanager email can be retrieved" in {
    mockLdapForDatamanager(Seq(correctDatamanagerAttrs))

    action.getDatamanagerEmailaddress(datamanagerId).value shouldBe "dm@test.org"
  }

  it should "fail if ldap does not return anything for the datamanager" in {
    mockLdapForDatamanager(Seq.empty)

    action.getDatamanagerEmailaddress(datamanagerId).leftValue.msg should
      include("The datamanager 'dm' is unknown")
  }

  it should "fail if ldap does not return multiple users" in {
    mockLdapForDatamanager(Seq(correctDatamanagerAttrs, correctDatamanagerAttrs))

    action.getDatamanagerEmailaddress(datamanagerId).leftValue.msg shouldBe
      s"There appear to be multiple users with id '$datamanagerId'"
  }

  it should "fail when the datamanager is not an active user" in {
    val nonActiveDatamanagerAttrs = createDatamanagerAttributes(state = "BLOCKED")
    mockLdapForDatamanager(Seq(nonActiveDatamanagerAttrs))

    action.getDatamanagerEmailaddress(datamanagerId).leftValue.msg should
      include("not an active user")
  }

  it should "fail when the datamanager is not an achivist" in {
    val nonArchivistDatamanagerAttrs = createDatamanagerAttributes(roles = Seq("USER"))
    mockLdapForDatamanager(Seq(nonArchivistDatamanagerAttrs))

    action.getDatamanagerEmailaddress(datamanagerId).leftValue.msg should
      include("is not an archivist")
  }

  it should "fail when the datamanager has no email" in {
    val nonEmailDatamanagerAttrs = createDatamanagerAttributes(mail = "")
    mockLdapForDatamanager(Seq(nonEmailDatamanagerAttrs))

    action.getDatamanagerEmailaddress(datamanagerId).leftValue.msg should
      include("does not have an email address")
  }
}
