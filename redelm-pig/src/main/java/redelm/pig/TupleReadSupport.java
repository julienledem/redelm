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

import java.util.List;

import redelm.hadoop.MetaDataBlock;
import redelm.hadoop.ReadSupport;
import redelm.io.RecordMaterializer;
import redelm.parser.MessageTypeParser;
import redelm.pig.converter.MessageConverter;
import redelm.schema.MessageType;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

/**
 * Read support for Pig Tuple
 * a Pig MetaDataBlock is expected in the initialization call
 *
 * @author Julien Le Dem
 *
 */
public class TupleReadSupport extends ReadSupport<Tuple> {
  private static final long serialVersionUID = 1L;

  private Schema pigSchema;
  private String requestedSchema;

  /**
   * {@inheritDoc}
   */
  @Override
  public void initForRead(List<MetaDataBlock> metaDataBlocks, String requestedSchema) {
    this.requestedSchema = requestedSchema;
    PigMetaData pigMetaData = PigMetaData.fromMetaDataBlocks(metaDataBlocks);
    try {
      this.pigSchema = Utils.getSchemaFromString(pigMetaData.getPigSchema());
    } catch (ParserException e) {
      throw new RuntimeException("could not parse Pig schema: " + pigMetaData.getPigSchema(), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordMaterializer<Tuple> newRecordConsumer() {
    MessageType redelmSchema = MessageTypeParser.parseMessageType(requestedSchema);
    MessageConverter converter = newParsingTree(redelmSchema, pigSchema);
    return converter.newRecordConsumer();
//    return new TupleRecordConsumer(
//        redelmSchema,
//        pigSchema,
//        destination);
  }

  private MessageConverter newParsingTree(MessageType redelmSchema, Schema pigSchema) {
    try {
      return new MessageConverter(redelmSchema, pigSchema);
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }

}
