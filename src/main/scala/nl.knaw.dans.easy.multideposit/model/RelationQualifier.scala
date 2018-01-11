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
