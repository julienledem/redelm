package redelm.column.primitive;

import java.io.IOException;

import redelm.column.BytesOutput;

/**
 * This is a special ColumnWriter for the case when you need to write
 * integers in a known range. This is intended primarily for use with
 * repetition and definition levels, since the maximum value that will
 * be written is known a priori based on the schema.
 */
public class BoundedIntColumnWriter extends PrimitiveColumnWriter {
  private int bound;

  public BoundedIntColumnWriter(int bound) {
    this.bound = bound;
  }

  @Override
  public int getMemSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void writeData(BytesOutput out) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub

  }

  @Override
  public void writeInt(int val) {

  }
}