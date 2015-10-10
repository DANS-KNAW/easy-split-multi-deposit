package nl.knaw.dans.easy.ps

import java.io.{File, FileInputStream}

import com.github.sardine.{Sardine, SardineFactory}
import com.github.sardine.impl.SardineException

import scala.util.{Failure, Success, Try}

trait StorageConnector {

  def exists(url: String): Try[Boolean]
  
  def createDirectory(url: String): Try[Unit]
  
  def writeFileTo(file: File, url: String): Try[Unit]
  
  def delete(url: String): Try[Unit]
}

object StorageConnector {

  def apply(user: String, password: String): StorageConnector = new WebDavConnector(user, password)

}

private class WebDavConnector(user: String, password: String) extends StorageConnector {
  
  override def exists(url: String): Try[Boolean] =
    runSardine(_.exists(url))

  override def createDirectory(url: String): Try[Unit] =
    runSardine(_.createDirectory(url))

  override def writeFileTo(file: File, url: String): Try[Unit] =
    runSardine(_.put(url, new FileInputStream(file)))
  
  override def delete(url: String): Try[Unit] =
    runSardine(_.delete(url))

  private def runSardine[T](f: Sardine => T): Try[T] =
    try
      Success(f(SardineFactory.begin(user, password)))
    catch {
      case e: SardineException => Failure(e)
    }

}