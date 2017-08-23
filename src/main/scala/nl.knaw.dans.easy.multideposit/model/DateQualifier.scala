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

object DateQualifier extends Enumeration {

  type DateQualifier = Value
  // @formatter:off
  val VALID: Value            = Value("valid")
  val ISSUED: Value           = Value("issued")
  val MODIFIED: Value         = Value("modified")
  val DATE_ACCEPTED: Value    = Value("dateAccepted")
  val DATE_COPYRIGHTED: Value = Value("dateCopyrighted")
  val DATE_SUBMITTED: Value   = Value("dateSubmitted")
  // @formatter:on

  def valueOf(s: String): Option[DateQualifier.Value] = {
    DateQualifier.values.find(_.toString equalsIgnoreCase s.replace(" ", ""))
  }
}
