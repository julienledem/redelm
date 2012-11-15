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
package redelm.pig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import redelm.hadoop.MetaDataBlock;
import redelm.hadoop.WriteSupport;
import redelm.io.RecordConsumer;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

public class TupleWriteSupport extends WriteSupport<Tuple> {
  private static final TupleFactory TF = TupleFactory.getInstance();

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;

  public void initForWrite(RecordConsumer recordConsumer, MessageType schema, List<MetaDataBlock> extraMetaData) {
    this.recordConsumer = recordConsumer;
    this.rootSchema = schema;
  }

  public void write(Tuple t) {
    try {
      recordConsumer.startMessage();
      writeTuple(rootSchema, t);
      recordConsumer.endMessage();
    } catch (ExecException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeTuple(GroupType schema, Tuple t) throws ExecException {
    List<Type> fields = schema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      if (!t.isNull(i)) {
        Type fieldType = fields.get(i);
        if (fieldType.getRepetition() == Repetition.REPEATED) {
          Object repeated = t.get(i);
          if (repeated instanceof DataBag) {
            DataBag bag = (DataBag)repeated;
            if (bag.size() > 0) {
              recordConsumer.startField(fieldType.getName(), i);
              for (Tuple tuple : bag) {
                if (fieldType.isPrimitive()) {
                  writeValue(fieldType, tuple, 0);
                } else {
                  recordConsumer.startGroup();
                  writeTuple(fieldType.asGroupType(), tuple);
                  recordConsumer.endGroup();
                }
              }
              recordConsumer.endField(fieldType.getName(), i);
            }
          } else if (repeated instanceof Map) {
            @SuppressWarnings("unchecked") // I know
            Map<String, Object> map = (Map<String, Object>)repeated;
            if (map.size() > 0) {
              recordConsumer.startField(fieldType.getName(), i);
              Set<Entry<String, Object>> entrySet = map.entrySet();
              for (Entry<String, Object> entry : entrySet) {
                recordConsumer.startGroup();
                writeTuple(fieldType.asGroupType(), TF.newTuple(Arrays.asList(entry.getKey(), entry.getValue())));
                recordConsumer.endGroup();
              }
              recordConsumer.endField(fieldType.getName(), i);
            }
          } else {
            throw new RuntimeException("not a supported repeated type: "+repeated.getClass());
          }
        } else {
          recordConsumer.startField(fieldType.getName(), i);
          writeValue(fieldType, t, i);
          recordConsumer.endField(fieldType.getName(), i);
        }
      }
    }
  }

  private void writeValue(Type type, Tuple t, int i) {
    try {
      if (type.isPrimitive()) {
        switch (type.asPrimitiveType().getPrimitive()) {
        // TODO: use PrimitiveTuple accessors
        case BINARY:
          recordConsumer.addBinary(((DataByteArray)t.get(i)).get());
          break;
        case BOOLEAN:
          recordConsumer.addBoolean((Boolean)t.get(i));
          break;
        case INT32:
          recordConsumer.addInteger(((Number)t.get(i)).intValue());
          break;
        case INT64:
          recordConsumer.addLong(((Number)t.get(i)).longValue());
          break;
        case STRING:
          recordConsumer.addString((String)t.get(i));
          break;
        case DOUBLE:
          recordConsumer.addDouble(((Number)t.get(i)).doubleValue());
          break;
        case FLOAT:
          recordConsumer.addFloat(((Number)t.get(i)).floatValue());
          break;
        default:
          throw new UnsupportedOperationException(type.asPrimitiveType().getPrimitive().name());
        }
      } else {
        recordConsumer.startGroup();
        writeTuple(type.asGroupType(), (Tuple)t.get(i));
        recordConsumer.endGroup();
      }
    } catch (Exception e) {
      throw new RuntimeException("can not write value at "+i+" for type "+type+" in tuple "+t, e);
    }
  }

}
