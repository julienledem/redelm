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
package redelm.schema;

import redelm.column.ColumnReader;
import redelm.io.RecordConsumer;

/**
 *
 * Representation of a Primitive type
 *
 * @author Julien Le Dem
 *
 */
public class PrimitiveType extends Type {
  /**
   * Supported Primitive types
   *
   * @author Julien Le Dem
   *
   */
  public static enum Primitive {
    STRING {
      @Override
      public String toString(ColumnReader columnReader) {
        return columnReader.getString();
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addString(columnReader.getString());
      }
    },
    INT64 {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getLong());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addLong(columnReader.getLong());
      }
    },
    INT32 {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getInteger());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addInteger(columnReader.getInteger());
      }
    },
    BOOLEAN {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBoolean());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBoolean(columnReader.getBoolean());
      }
    },
    BINARY {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBinary());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBinary(columnReader.getBinary());
      }
    },
    FLOAT {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getFloat());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addFloat(columnReader.getFloat());
      }
    },
    DOUBLE {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getDouble());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addDouble(columnReader.getDouble());
      }
    };

    /**
     * reads the value from the columnReader with the appropriate accessor and returns a String representation
     * @param columnReader
     * @return a string
     */
    abstract public String toString(ColumnReader columnReader);

    /**
     * reads the value from the columnReader with the appropriate accessor and writes it to the recordConsumer
     * @param recordConsumer where to write
     * @param columnReader where to read from
     */
    abstract public void addValueToRecordConsumer(RecordConsumer recordConsumer,
        ColumnReader columnReader);

  }

  private final Primitive primitive;

  /**
   *
   * @param repetition the OPTIONAL, REPEATED, REQUIRED
   * @param primitive STRING, INT64, ...
   * @param name the name of the type
   */
  public PrimitiveType(Repetition repetition, Primitive primitive, String name) {
    super(name, repetition);
    this.primitive = primitive;
  }

  /**
   * @return the primitive type
   */
  public Primitive getPrimitive() {
    return primitive;
  }

  /**
   * @return true
   */
  @Override
  public boolean isPrimitive() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accept(TypeVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeToStringBuilder(StringBuilder sb, String indent) {
    sb.append(indent)
        .append(getRepetition().name().toLowerCase())
        .append(" ")
        .append(primitive.name().toLowerCase())
        .append(" ")
        .append(getName());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean typeEquals(Type other) {
    if (other.isPrimitive()) {
      PrimitiveType primitiveType = other.asPrimitiveType();
      return getRepetition() == primitiveType.getRepetition() &&
          getPrimitive().equals(primitiveType.getPrimitive()) &&
          getName().equals(primitiveType.getName());
    } else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected int typeHashCode() {
    int hash = 17;
    hash += 31 * getRepetition().hashCode();
    hash += 31 * getPrimitive().hashCode();
    hash += 31 * getName().hashCode();
    return hash;
  }
}
