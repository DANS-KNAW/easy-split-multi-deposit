package nl.knaw.dans.easy.multideposit2

import javax.naming.directory.{ Attributes, SearchControls }
import javax.naming.ldap.LdapContext

import scala.util.Try
import scala.collection.JavaConverters._

trait Ldap extends AutoCloseable {

  protected val ctx: LdapContext

  def ldapQuery[T](userId: String)(f: Attributes => T): Try[Seq[T]] = Try {
    val searchFilter = s"(&(objectClass=easyUser)(uid=$userId))"
    val searchControls = new SearchControls() {
      setSearchScope(SearchControls.SUBTREE_SCOPE)
    }

    ctx.search("dc=dans,dc=knaw,dc=nl", searchFilter, searchControls)
      .asScala.toSeq
      .map(f compose (_.getAttributes))
  }

  override def close(): Unit = ctx.close()
}
object Ldap {
  def apply(context: LdapContext): Ldap = new Ldap {
    override protected val ctx: LdapContext = context
  }
}
