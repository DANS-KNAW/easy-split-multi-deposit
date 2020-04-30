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
package nl.knaw.dans.easy.multideposit.parser

import nl.knaw.dans.easy.multideposit.TestSupportFixture

class ParserPackageSpec extends TestSupportFixture {

  "find" should "return the value corresponding to the key in the row" in {
    val row = DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def"))
    row.find(Headers.Title).value shouldBe "abc"
  }

  it should "return None if the key is not present in the row" in {
    val row = DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def"))
    row.find(Headers.Creator) shouldBe empty
  }

  it should "return None if the value corresponding to the key in the row is empty" in {
    val row = DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> ""))
    row.find(Headers.Description) shouldBe empty
  }

  it should "return None if the value corresponding to the key in the row is blank" in {
    val row = DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "  \t  "))
    row.find(Headers.Description) shouldBe empty
  }
}
