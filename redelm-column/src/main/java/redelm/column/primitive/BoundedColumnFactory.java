package redelm.column.primitive;

public class BoundedColumnFactory {
  public static PrimitiveColumnReader getBoundedReader(int bound) {
    return bound == 0 ? new DevNullColumnReader() : new BoundedIntColumnReader(bound);
  }

  public static PrimitiveColumnWriter getBoundedWriter(int bound) {
    return bound == 0 ? new DevNullColumnWriter() : new BoundedIntColumnWriter(bound);
  }
}
