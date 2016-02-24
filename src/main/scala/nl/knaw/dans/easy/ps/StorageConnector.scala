/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.ps

import java.io.{File, FileInputStream}

import com.github.sardine.{Sardine, SardineFactory}
import com.github.sardine.impl.SardineException

import scala.util.{Failure, Success, Try}

@Deprecated
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
