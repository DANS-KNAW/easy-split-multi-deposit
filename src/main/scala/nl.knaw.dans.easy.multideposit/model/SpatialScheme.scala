/*
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

object SpatialScheme extends Enumeration {

  type SpatialScheme = Value
  // @formatter:off
  val ISO3166: Value = Value("dcterms:ISO3166")
  // @formatter:on

  def valueOf(s: String): Option[SpatialScheme.Value] = {
    SpatialScheme.values.find(_.toString equalsIgnoreCase s)
  }
}
