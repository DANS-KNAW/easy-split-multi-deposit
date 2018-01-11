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

object ContributorRole extends Enumeration {

  type ContributorRole = Value
  // @formatter:off
  val CONTACT_PERSON: Value         = Value("ContactPerson")
  val DATA_COLLECTOR: Value         = Value("DataCollector")
  val DATA_CURATOR: Value           = Value("DataCurator")
  val DATA_MANAGER: Value           = Value("DataManager")
  val DISTRIBUTOR: Value            = Value("Distributor")
  val EDITOR: Value                 = Value("Editor")
  val HOSTING_INSTITUTION: Value    = Value("HostingInstitution")
  val OTHER: Value                  = Value("Other")
  val PRODUCER: Value               = Value("Producer")
  val PROJECT_LEADER: Value         = Value("ProjectLeader")
  val PROJECT_MANAGER: Value        = Value("ProjectManager")
  val PROJECT_MEMBER: Value         = Value("ProjectMember")
  val REGISTRATION_AGENCY: Value    = Value("RegistrationAgency")
  val REGISTRATION_AUTHORITY: Value = Value("RegistrationAuthority")
  val RELATED_PERSON: Value         = Value("RelatedPerson")
  val RESEARCH_GROUP: Value         = Value("ResearchGroup")
  val RIGHTS_HOLDER: Value          = Value("RightsHolder")
  val RESEARCHER: Value             = Value("Researcher")
  val SPONSOR: Value                = Value("Sponsor")
  val SUPERVISOR: Value             = Value("Supervisor")
  val WORK_PACKAGE_LEADER: Value    = Value("WorkPackageLeader")
  // @formatter:on

  def valueOf(s: String): Option[ContributorRole.Value] = {
    ContributorRole.values.find(_.toString equalsIgnoreCase s.replace(" ", ""))
  }
}
