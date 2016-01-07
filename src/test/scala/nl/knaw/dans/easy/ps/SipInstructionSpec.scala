/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.easy.ps

import org.apache.commons.io.FileUtils._

class SipInstructionSpec extends UnitSpec {

  "parse" should "complain about with garbage in the instructions file" in {

    val instructionsFile = file(testDir, "instructions.csv")
    write(instructionsFile, "rabarabera")

    (the [ActionException] thrownBy SipInstructions.parse(instructionsFile)
      .toBlocking.toList).message should include ("unknown headers: rabarabera")
  }

  it should "fail with empty instructions file?" in {

    val instructionsFile = file(testDir, "sip/instructions.csv")
    write(instructionsFile, "")

    the[NoSuchElementException] thrownBy
    SipInstructions.parse(instructionsFile) should have message "next on empty iterator"
  }

  it should "fail without DATASET_ID in instructions file?" in {

    val instructionsFile = file(testDir, "instructions.csv")
    write(instructionsFile, "SF_PRESENTATION,FILE_AUDIO_VIDEO\nx,y")

    the[Exception] thrownBy SipInstructions.parse(instructionsFile)
      .toBlocking.toList should have message "java.lang.Exception: No dataset ID found"
  }

  it should "not complain about an invalid combination in instructions file?" in {

    val instructionsFile = file(testDir, "instructions.csv")
    write(instructionsFile, "DATASET_ID,FILE_SIP\ndataset1,x")

    SipInstructions.parse(instructionsFile).toBlocking.toList.head.head._1 shouldBe "dataset1"
  }

  it should "succeed with Roundtrip/sip-demo-2015-02-24" in {

    val instructionsFile = file("src/test/resources/Roundtrip/sip-demo-2015-02-24/instructions.csv")
    SipInstructions.parse(instructionsFile).toBlocking.toList.head.head._1 shouldBe "ruimtereis01"
    // further checks by MainSpec
  }
}
