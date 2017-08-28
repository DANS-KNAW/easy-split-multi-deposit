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

object DcType extends Enumeration {

  type DcType = Value
  // @formatter:off
  val COLLECTION: Value          = Value("Collection")
  val DATASET: Value             = Value("Dataset")
  val EVENT: Value               = Value("Event")
  val IMAGE: Value               = Value("Image")
  val INTERACTIVERESOURCE: Value = Value("InteractiveResource")
  val MOVINGIMAGE: Value         = Value("MovingImage")
  val PHYSICALOBJECT: Value      = Value("PhysicalObject")
  val SERVICE: Value             = Value("Service")
  val SOFTWARE: Value            = Value("Software")
  val SOUND: Value               = Value("Sound")
  val STILLIMAGE: Value          = Value("StillImage")
  val TEXT: Value                = Value("Text")
  // @formatter:on

  def valueOf(s: String): Option[DcType.Value] = {
    DcType.values.find(_.toString equalsIgnoreCase s.replace(" ", ""))
  }
}
