package nl.knaw.dans.easy.multideposit.model

object DateQualifier extends Enumeration {

  type DateQualifier = Value

  val VALID            = Value("valid")
  val ISSUED           = Value("issued")
  val MODIFIED         = Value("modified")
  val DATE_ACCEPTED    = Value("dateAccepted")
  val DATE_COPYRIGHTED = Value("dateCopyrighted")
  val DATE_SUBMITTED   = Value("dateSubmitted")

  def valueOf(s: String): Option[DateQualifier.Value] = {
    val value = s.toLowerCase.replace(" ", "")
    DateQualifier.values.find(_.toString.toLowerCase() == value)
  }
}
