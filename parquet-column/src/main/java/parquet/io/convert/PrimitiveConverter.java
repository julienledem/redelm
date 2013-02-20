/**
 * Copyright 2012 Twitter, Inc.
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
package parquet.io.convert;

abstract public class PrimitiveConverter {

  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addBinary(byte[] value) {
    throw new UnsupportedOperationException();
  }
  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addBoolean(boolean value) {
    throw new UnsupportedOperationException();
  }
  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addDouble(double value) {
    throw new UnsupportedOperationException();
  }
  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addFloat(float value) {
    throw new UnsupportedOperationException();
  }
  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addInt(int value) {
    throw new UnsupportedOperationException();
  }
  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addLong(long value) {
    throw new UnsupportedOperationException();
  }

}
