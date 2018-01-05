package nl.knaw.dans.easy.multideposit2

package object actions {

  case class ActionException(msg: String, cause: Throwable = null) extends Exception(msg, cause)
}
