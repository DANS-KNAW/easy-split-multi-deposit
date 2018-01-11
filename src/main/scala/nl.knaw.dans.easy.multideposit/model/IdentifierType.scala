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

object IdentifierType extends Enumeration {

  type IdentifierType = Value
  // @formatter:off
  val ISBN: Value                      = Value("ISBN")
  val ISSN: Value                      = Value("ISSN")
  val NWO_PROJECTNR: Value             = Value("NWO-PROJECTNR")
  val ARCHIS_ZAAK_IDENTIFICATIE: Value = Value("ARCHIS-ZAAK-IDENTIFICATIE")
  val E_DNA: Value                     = Value("eDNA-project")
  // @formatter:on

  def valueOf(s: String): Option[IdentifierType.Value] = {
    IdentifierType.values.find(_.toString equalsIgnoreCase s)
  }
}
