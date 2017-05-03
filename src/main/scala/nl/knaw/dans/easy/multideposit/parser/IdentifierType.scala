package nl.knaw.dans.easy.multideposit.parser

object IdentifierType extends Enumeration {

  type IdentifierType = Value
  val ISBN = Value
  val ISSN = Value
  val NWO_PROJECTNR = Value
  val ARCHIS_ZAAK_IDENTIFICATIE = Value

  def valueOf(s: String): Option[IdentifierType.Value] = {
    IdentifierType.values.find(_.toString == s)
  }
}
