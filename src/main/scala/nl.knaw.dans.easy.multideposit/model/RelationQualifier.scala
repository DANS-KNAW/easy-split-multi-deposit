package nl.knaw.dans.easy.multideposit.model

object RelationQualifier extends Enumeration {

  type RelationQualifier = Value
  // @formatter:off
  val IsVersionOf: Value    = Value("isVersionOf")
  val HasVersion: Value     = Value("hasVersion")
  val IsReplacedBy: Value   = Value("isReplacedBy")
  val Replaces: Value       = Value("replaces")
  val IsRequiredBy: Value   = Value("isRequiredBy")
  val Requires: Value       = Value("requires")
  val IsPartOf: Value       = Value("isPartOf")
  val HasPart: Value        = Value("hasPart")
  val IsReferencedBy: Value = Value("isReferencedBy")
  val References: Value     = Value("references")
  val IsFormatOf: Value     = Value("isFormatOf")
  val HasFormat: Value      = Value("hasFormat")
  val ConformsTo: Value     = Value("conformsTo")
  // @formatter:on

  def valueOf(s: String): Option[RelationQualifier.Value] = {
    RelationQualifier.values.find(_.toString equalsIgnoreCase s.replace(" ", ""))
  }
}
