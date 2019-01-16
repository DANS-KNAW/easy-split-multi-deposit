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

import cats.data.NonEmptyList
import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.model.ContributorRole.ContributorRole
import org.joda.time.DateTime

case class Profile(titles: NonEmptyList[String],
                   descriptions: NonEmptyList[String],
                   creators: NonEmptyList[Creator],
                   created: DateTime,
                   available: DateTime = DateTime.now(),
                   audiences: NonEmptyList[String], // or List[enum values]?
                   accessright: AccessCategory) // only one allowed? not yet in validation

sealed abstract class Creator
case class CreatorOrganization(organization: String,
                               role: Option[ContributorRole] = Option.empty) extends Creator
case class CreatorPerson(titles: Option[String] = Option.empty,
                         initials: String,
                         insertions: Option[String] = Option.empty,
                         surname: String,
                         organization: Option[String] = Option.empty,
                         role: Option[ContributorRole] = Option.empty,
                         dai: Option[String] = Option.empty) extends Creator
