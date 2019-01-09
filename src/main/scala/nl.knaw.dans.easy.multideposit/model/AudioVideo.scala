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

import better.files.File
import nl.knaw.dans.easy.multideposit.model.PlayMode.PlayMode

case class AudioVideo(springfield: Option[Springfield] = Option.empty,
                      avFiles: Map[File, Set[SubtitlesFile]] = Map.empty)

case class Springfield(domain: String = "dans",
                       user: String,
                       collection: String,
                       playMode: PlayMode) {
  def toTuple: (String, String, String, PlayMode) = (domain, user, collection, playMode)
}
object Springfield {

  def maybeWithDomain(domain: Option[String], user: String, collection: String, playMode: PlayMode): Springfield = {
    domain.map(Springfield(_, user, collection, playMode))
      .getOrElse(Springfield(user = user, collection = collection, playMode = playMode))
  }
}

case class SubtitlesFile(file: File, language: Option[String] = Option.empty)
