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
package redelm.column;

import java.util.Arrays;

import redelm.schema.PrimitiveType.PrimitiveTypeName;

public class ColumnDescriptor implements Comparable<ColumnDescriptor> {

  private final String[] path;
  private final PrimitiveTypeName type;
  private final int maxRep;
  private final int maxDef;

  //TODO see if we actually need the maxRep and maxDef here
  public ColumnDescriptor(String[] path, PrimitiveTypeName type, int maxRep, int maxDef) {
    super();
    this.path = path;
    this.type = type;
    this.maxRep = maxRep;
    this.maxDef = maxDef;
  }

  public String[] getPath() {
    return path;
  }

  public int getRepetitionLevel() {
    return maxRep;
  }

  public int getDefinitionLevel() {
    return maxDef;
  }

  public PrimitiveTypeName getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(path);
  }

  @Override
  public boolean equals(Object obj) {
    return Arrays.equals(path, ((ColumnDescriptor)obj).path);
  }

  @Override
  public int compareTo(ColumnDescriptor o) {
    for (int i = 0; i < path.length; i++) {
      int compareTo = path[i].compareTo(o.path[i]);
      if (compareTo != 0) {
        return compareTo;
      }
    }
    return 0;
  }

  @Override
  public String toString() {
    return Arrays.toString(path)+" "+type;
  }
}