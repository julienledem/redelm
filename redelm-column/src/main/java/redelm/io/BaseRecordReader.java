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
package redelm.io;

import static redelm.Log.DEBUG;
import redelm.Log;
import redelm.column.ColumnsStore;
import redelm.io.RecordReaderImplementation.State;

public abstract class BaseRecordReader<T> extends RecordReader<T> {
  private static final Log LOG = Log.getLog(BaseRecordReader.class);

  public RecordConsumer recordConsumer;
  public RecordMaterializer<T> recordMaterializer;
  public ColumnsStore columnStore;
  @Override
  public T read() {
    readOneRecord();
    return recordMaterializer.getCurrentRecord();
  }

  /* (non-Javadoc)
   * @see redelm.io.RecordReader#read(T[], int)
   */
  @Override
  public void read(T[] records, int count) {
    if (count > records.length) {
      throw new IllegalArgumentException("count is greater than records size: " + count + " > " + records.length);
    }
    for (int i = 0; i < count; i++) {
      readOneRecord();
      records[i] = recordMaterializer.getCurrentRecord();
    }
  }

  protected abstract void readOneRecord();

  State[] caseLookup;

  private String endField;

  private int endIndex;

  protected void currentLevel(int currentLevel) {
    if (DEBUG) LOG.debug("currentLevel: "+currentLevel);
  }

  protected void log(String message) {
    if (DEBUG) LOG.debug("bc: "+message);
  }

  final protected int getCaseId(int state, int currentLevel, int d, int nextR) {
    return caseLookup[state].getCase(currentLevel, d, nextR).getID();
  }

  final protected void startMessage() {
    // reset state
    endField = null;
    if (DEBUG) LOG.debug("startMessage()");
    recordConsumer.startMessage();
  }

  final protected void startGroup(String field, int index) {
    startField(field, index);
    if (DEBUG) LOG.debug("startGroup()");
    recordConsumer.startGroup();
  }

  private void startField(String field, int index) {
    if (DEBUG) LOG.debug("startField("+field+","+index+")");
    if (endField != null && index == endIndex) {
      // skip the close/open tag
      endField = null;
    } else {
      if (endField != null) {
        // close the previous field
        recordConsumer.endField(endField, endIndex);
        endField = null;
      }
      recordConsumer.startField(field, index);
    }
  }

  final protected void addPrimitiveINT64(String field, int index, long value) {
    startField(field, index);
    if (DEBUG) LOG.debug("addLong("+value+")");
    recordConsumer.addLong(value);
    endField(field, index);
  }

  private void endField(String field, int index) {
    if (DEBUG) LOG.debug("endField("+field+","+index+")");
    if (endField != null) {
      recordConsumer.endField(endField, endIndex);
    }
    endField = field;
    endIndex = index;
  }

  final protected void addPrimitiveSTRING(String field, int index, String value) {
    startField(field, index);
    if (DEBUG) LOG.debug("addString("+value+")");
    recordConsumer.addString(value);
    endField(field, index);
  }

  final protected void addPrimitiveINT32(String field, int index, int value) {
    startField(field, index);
    if (DEBUG) LOG.debug("addInteger("+value+")");
    recordConsumer.addInteger(value);
    endField(field, index);
  }

  final protected void endGroup(String field, int index) {
    if (endField != null) {
      // close the previous field
      recordConsumer.endField(endField, endIndex);
      endField = null;
    }
    if (DEBUG) LOG.debug("endGroup()");
    recordConsumer.endGroup();
    endField(field, index);
  }

  final protected void endMessage() {
    if (endField != null) {
      // close the previous field
      recordConsumer.endField(endField, endIndex);
      endField = null;
    }
    if (DEBUG) LOG.debug("endMessage()");
    recordConsumer.endMessage();
  }

  protected void error(String message) {
    throw new RuntimeException(message);
  }
}