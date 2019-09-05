/**
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
package nl.knaw.dans.easy.multideposit

import java.nio.file.Paths

import better.files.File
import better.files.File.root
import org.apache.commons.configuration.PropertiesConfiguration

import scala.collection.JavaConverters._

case class Configuration(version: String, properties: PropertiesConfiguration, formats: Set[String], licenses: Set[String])

object Configuration {

  def apply(home: File): Configuration = {
    val cfgPath = Seq(
      root / "etc" / "opt" / "dans.knaw.nl" / "easy-split-multi-deposit",
      home / "cfg")
      .find(_.exists)
      .getOrElse { throw new IllegalStateException("No configuration directory found") }

    val formatsFile = cfgPath / "acceptedMediaTypes.txt"
    val licenses = new PropertiesConfiguration(Paths.get(home.toString()).resolve("lic/licenses.properties").toFile)

    new Configuration(
      version = (home / "bin" / "version").contentAsString.stripLineEnd,
      properties = new PropertiesConfiguration() {
        setDelimiterParsingDisabled(true)
        load((cfgPath / "application.properties").toJava)
      },
      formats =
        if (formatsFile.exists) formatsFile.lines.map(_.trim).toSet
        else Set.empty,
      licenses = licenses.getKeys.asScala.filterNot(_.isEmpty).toSet
    )
  }
}
