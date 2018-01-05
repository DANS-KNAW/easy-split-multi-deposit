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

case class Instructions(depositId: DepositId,
                        row: Int,
                        depositorUserId: DepositorUserId,
                        profile: Profile,
                        metadata: Metadata = Metadata(),
                        files: Map[Path, FileDescriptor] = Map.empty,
                        audioVideo: AudioVideo = AudioVideo()) {
  def toDeposit(fms: Seq[FileMetadata]): Deposit = {
    Deposit(depositId, row, depositorUserId, profile, metadata, fms, audioVideo.springfield)
  }
}

case class Deposit(depositId: DepositId,
                   row: Int,
                   depositorUserId: DepositorUserId,
                   profile: Profile,
                   metadata: Metadata = Metadata(),
                   files: Seq[FileMetadata] = Seq.empty,
                   springfield: Option[Springfield] = Option.empty)