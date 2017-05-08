package nl.knaw.dans.easy.multideposit.parser

object IdentifierType extends Enumeration {

  type IdentifierType = Value
  val ISBN = Value("ISBN")
  val ISSN = Value("ISSN")
  val NWO_PROJECTNR = Value("NWO-PROJECTNR")
  val ARCHIS_ZAAK_IDENTIFICATIE = Value("ARCHIS-ZAAK-IDENTIFICATIE")

  def valueOf(s: String): Option[IdentifierType.Value] = {
    IdentifierType.values.find(_.toString == s)
  }
}
