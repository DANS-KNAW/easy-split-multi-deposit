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
package nl.knaw.dans.easy.multideposit2.model

import java.nio.file.Path

import nl.knaw.dans.easy.multideposit2.model.PlayMode.PlayMode

case class AudioVideo(springfield: Option[Springfield] = Option.empty,
                      avFiles: Map[Path, Set[Subtitles]] = Map.empty)

case class Springfield(domain: String = "dans",
                       user: String,
                       collection: String,
                       playMode: PlayMode = PlayMode.Continuous)

case class Subtitles(path: Path, language: Option[String] = Option.empty)
