package nl.knaw.dans.easy.multideposit2.actions

import javax.naming.directory.{ Attributes, BasicAttribute, BasicAttributes }

import nl.knaw.dans.easy.multideposit2.{ Ldap, TestSupportFixture }
import org.scalamock.scalatest.MockFactory

import scala.util.{ Failure, Success, Try }

class RetrieveDatamanagerSpec extends TestSupportFixture with MockFactory {

  val ldapMock: Ldap = mock[Ldap]
  val datamanagerId = "dm"
  val action: RetrieveDatamanager = new RetrieveDatamanager {
    val ldap: Ldap = ldapMock
  }

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

  def mockLdapForDatamanager(attrs: Try[Seq[Attributes]]): Unit = {
    (ldapMock.ldapQuery(_: String)(_: Attributes => Attributes)) expects("dm", *) once() returning attrs
  }

  "getDatamanagerEmailaddress" should "succeed if the datamanager email can be retrieved" in {
    mockLdapForDatamanager(Success(Seq(correctDatamanagerAttrs)))

    action.getDatamanagerEmailaddress(datamanagerId) should matchPattern {
      case Success("dm@test.org") =>
    }
  }

  it should "fail if ldap does not return anything for the datamanager" in {
    mockLdapForDatamanager(Success(Seq.empty))

    inside(action.getDatamanagerEmailaddress(datamanagerId)) {
      case Failure(ActionException(message, _)) => message should include("The datamanager 'dm' is unknown")
    }
  }

  it should "fail if ldap does not return multiple users" in {
    mockLdapForDatamanager(Success(Seq(correctDatamanagerAttrs, correctDatamanagerAttrs)))

    inside(action.getDatamanagerEmailaddress(datamanagerId)) {
      case Failure(ActionException(message, _)) => message shouldBe s"There appear to be multiple users with id '$datamanagerId'"
    }
  }

  it should "fail when the datamanager is not an active user" in {
    val nonActiveDatamanagerAttrs = createDatamanagerAttributes(state = "BLOCKED")
    mockLdapForDatamanager(Success(Seq(nonActiveDatamanagerAttrs)))

    inside(action.getDatamanagerEmailaddress(datamanagerId)) {
      case Failure(ActionException(message, _)) => message should include("not an active user")
    }
  }

  it should "fail when the datamanager is not an achivist" in {
    val nonArchivistDatamanagerAttrs = createDatamanagerAttributes(roles = Seq("USER"))
    mockLdapForDatamanager(Success(Seq(nonArchivistDatamanagerAttrs)))

    inside(action.getDatamanagerEmailaddress(datamanagerId)) {
      case Failure(ActionException(message, _)) => message should include("is not an archivist")
    }
  }

  it should "fail when the datamanager has no email" in {
    val nonEmailDatamanagerAttrs = createDatamanagerAttributes(mail = "")
    mockLdapForDatamanager(Success(Seq(nonEmailDatamanagerAttrs)))

    inside(action.getDatamanagerEmailaddress(datamanagerId)) {
      case Failure(ActionException(message, _)) => message should include("does not have an email address")
    }
  }
}
