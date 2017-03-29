/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.easy.multideposit.actions

/**
 * Enumeration of the file properties VisibleTo and AccessibleTo
 * Partially copied from easy-stage-file-item
 */
// TODO move this enum to the DDM library
object FileAccessRights extends Enumeration {

  type UserCategory = Value
  val
  ANONYMOUS, // a user that is not logged in
  KNOWN, // a logged in user
  RESTRICTED_REQUEST, // a user that received permission to access the dataset
  RESTRICTED_GROUP, // a user belonging to the same group as the dataset
  NONE // none of the above
  = Value
}
