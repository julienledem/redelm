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
package redelm.hadoop;

import static redelm.Log.DEBUG;
import static redelm.bytes.BytesUtils.readIntLittleEndian;
import static redelm.hadoop.RedelmFileWriter.MAGIC;
import static redelm.hadoop.RedelmFileWriter.RED_ELM_SUMMARY;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import redelm.Log;
import redelm.bytes.BytesInput;
import redelm.column.ColumnDescriptor;
import redelm.column.mem.Page;
import redelm.column.mem.PageReadStore;
import redelm.format.converter.ParquetMetadataConverter;
import redelm.hadoop.CodecFactory.BytesDecompressor;
import redelm.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader;
import redelm.hadoop.metadata.BlockMetaData;
import redelm.hadoop.metadata.ColumnChunkMetaData;
import redelm.hadoop.metadata.RedelmMetaData;
import parquet.format.PageHeader;
import parquet.format.PageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils;

/**
 * Reads a RedElm file
 *
 * @author Julien Le Dem
 *
 */
public class RedelmFileReader {

  private static final Log LOG = Log.getLog(RedelmFileReader.class);

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private static RedelmMetaData deserializeFooter(InputStream is) throws IOException {
    parquet.format.FileMetaData parquetMetadata = parquetMetadataConverter.readFileMetaData(is);
    if (Log.DEBUG) LOG.debug(parquetMetadataConverter.toString(parquetMetadata));
    RedelmMetaData metadata = parquetMetadataConverter.fromParquetMetadata(parquetMetadata);
    if (Log.DEBUG) LOG.debug(RedelmMetaData.toPrettyJSON(metadata));
    return metadata;
  }

  /**
   * for files provided, check if there's a summary file.
   * If a summary file is found it is used otherwise the file footer is used.
   * @param configuration the hadoop conf to connect to the file system;
   * @param partFiles the part files to read
   * @return the footers for those files using the summary file if possible.
   * @throws IOException
   */
  public static List<Footer> readAllFootersInParallelUsingSummaryFiles(final Configuration configuration, List<FileStatus> partFiles) throws IOException {

    // figure out list of all parents to part files
    Set<Path> parents = new HashSet<Path>();
    for (FileStatus part : partFiles) {
      parents.add(part.getPath().getParent());
    }

    // read corresponding summary files if they exist
    Map<Path, Footer> cache = new HashMap<Path, Footer>();
    for (Path path : parents) {
      FileSystem fileSystem = path.getFileSystem(configuration);
      Path summaryFile = new Path(path, RED_ELM_SUMMARY);
      if (fileSystem.exists(summaryFile)) {
        List<Footer> footers = readSummaryFile(configuration, fileSystem.getFileStatus(summaryFile));
        for (Footer footer : footers) {
          cache.put(footer.getFile(), footer);
        }
      }
    }

    // keep only footers for files actually requested and read file footer if not found in summaries
    List<Footer> result = new ArrayList<Footer>(partFiles.size());
    List<FileStatus> toRead = new ArrayList<FileStatus>();
    for (FileStatus part : partFiles) {
      if (cache.containsKey(part.getPath())) {
        result.add(cache.get(part.getPath()));
      } else {
        toRead.add(part);
      }
    }

    // read the footers of the files that did not have a summary file
    result.addAll(readAllFootersInParallel(configuration, toRead));

    return result;
  }

  public static List<Footer> readAllFootersInParallel(final Configuration configuration, List<FileStatus> partFiles) throws IOException {
    ExecutorService threadPool = Executors.newFixedThreadPool(5);
    try {
      List<Future<Footer>> footers = new ArrayList<Future<Footer>>();
      for (final FileStatus currentFile : partFiles) {
        footers.add(threadPool.submit(new Callable<Footer>() {
          @Override
          public Footer call() throws Exception {
            try {
              FileSystem fs = currentFile.getPath().getFileSystem(configuration);
              return readFooter(fs, currentFile);
            } catch (IOException e) {
              throw new IOException("Could not read footer for file " + currentFile, e);
            }
          }

          private Footer readFooter(final FileSystem fs,
              final FileStatus currentFile) throws IOException {
            RedelmMetaData redelmMetaData = RedelmFileReader.readFooter(configuration, currentFile);
            return new Footer(currentFile.getPath(), redelmMetaData);
          }
        }));
      }
      List<Footer> result = new ArrayList<Footer>(footers.size());
      for (Future<Footer> futureFooter : footers) {
        try {
          result.add(futureFooter.get());
        } catch (InterruptedException e) {
          Thread.interrupted();
          throw new RuntimeException("The thread was interrupted", e);
        } catch (ExecutionException e) {
          throw new IOException("Could not read footer: " + e.getMessage(), e.getCause());
        }
      }
      return result;
    } finally {
      threadPool.shutdownNow();
    }
  }

  public static List<Footer> readAllFootersInParallel(Configuration configuration, FileStatus fileStatus) throws IOException {
    final FileSystem fs = fileStatus.getPath().getFileSystem(configuration);
    List<FileStatus> statuses;
    if (fileStatus.isDir()) {
      statuses = Arrays.asList(fs.listStatus(fileStatus.getPath(), new Utils.OutputFileUtils.OutputFilesFilter()));
    } else {
      statuses = new ArrayList<FileStatus>();
      statuses.add(fileStatus);
    }
    return readAllFootersInParallel(configuration, statuses);
  }

  public static List<Footer> readFooters(Configuration configuration, FileStatus pathStatus) throws IOException {
    try {
      if (pathStatus.isDir()) {
        Path summaryPath = new Path(pathStatus.getPath(), RED_ELM_SUMMARY);
        FileSystem fs = summaryPath.getFileSystem(configuration);
        if (fs.exists(summaryPath)) {
          FileStatus summaryStatus = fs.getFileStatus(summaryPath);
          return readSummaryFile(configuration, summaryStatus);
        }
      }
    } catch (IOException e) {
      LOG.warn("can not read summary file for " + pathStatus.getPath(), e);
    }
    return readAllFootersInParallel(configuration, pathStatus);

  }

  public static List<Footer> readSummaryFile(Configuration configuration, FileStatus summaryStatus) throws IOException {
    FileSystem fs = summaryStatus.getPath().getFileSystem(configuration);
    FSDataInputStream summary = fs.open(summaryStatus.getPath());
    int footerCount = summary.readInt();
    List<Footer> result = new ArrayList<Footer>(footerCount);
    for (int i = 0; i < footerCount; i++) {
      Path file = new Path(summary.readUTF());
      RedelmMetaData redelmMetaData = parquetMetadataConverter.fromParquetMetadata(parquetMetadataConverter.readFileMetaData(summary));
      result.add(new Footer(file, redelmMetaData));
    }
    summary.close();
    return result;
  }

  /**
   * Reads the meta data block in the footer of the file
   * @param configuration
   * @param file the RedElm File
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   */
  public static final RedelmMetaData readFooter(Configuration configuration, Path file) throws IOException {
    FileSystem fileSystem = file.getFileSystem(configuration);
    return readFooter(configuration, fileSystem.getFileStatus(file));
  }

  /**
   * Reads the meta data block in the footer of the file
   * @param configuration
   * @param file the RedElm File
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   */
  public static final RedelmMetaData readFooter(Configuration configuration, FileStatus file) throws IOException {
    FileSystem fileSystem = file.getPath().getFileSystem(configuration);
    FSDataInputStream f = fileSystem.open(file.getPath());
    long l = file.getLen();
    if (Log.DEBUG) LOG.debug("File length " + l);
    int FOOTER_LENGTH_SIZE = 4;
    if (l <= MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      throw new RuntimeException(file.getPath() + " is not a Red Elm file (too small)");
    }
    long footerLengthIndex = l - FOOTER_LENGTH_SIZE - MAGIC.length;
    if (Log.DEBUG) LOG.debug("reading footer index at " + footerLengthIndex);

    f.seek(footerLengthIndex);
    int footerLength = readIntLittleEndian(f);
    byte[] magic = new byte[MAGIC.length];
    f.readFully(magic);
    if (!Arrays.equals(MAGIC, magic)) {
      throw new RuntimeException(file.getPath() + " is not a RedElm file. expected magic number at tail " + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
    }
    long footerIndex = footerLengthIndex - footerLength;
    if (Log.DEBUG) LOG.debug("read footer length: " + footerLength + ", footer index: " + footerIndex);
    if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
      throw new RuntimeException("corrupted file: the footer index is not within the file");
    }
    f.seek(footerIndex);
    return deserializeFooter(f);

  }
  private CodecFactory codecFactory;

  private final List<BlockMetaData> blocks;
  private final FSDataInputStream f;
  private int currentBlock = 0;
  private Map<String, ColumnDescriptor> paths = new HashMap<String, ColumnDescriptor>();
  private long previousReadIndex = 0;


  /**
   *
   * @param f the redelm file
   * @param blocks the blocks to read
   * @param colums the columns to read (their path)
   * @param codecClassName the codec used to compress the blocks
   * @throws IOException if the file can not be opened
   */
  public RedelmFileReader(Configuration configuration, Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns) throws IOException {
    FileSystem fs = FileSystem.get(configuration);
    this.f = fs.open(filePath);
    this.blocks = blocks;
    for (ColumnDescriptor col : columns) {
      paths.put(Arrays.toString(col.getPath()), col);
    }
    this.codecFactory = new CodecFactory(configuration);
  }

  /**
   * reads all the columns requested in the next block
   * @return the block data for the next block
   * @throws IOException if an error occurs while reading
   * @return how many records where read or 0 if end reached.
   */
  public PageReadStore readColumns() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }
    ColumnChunkPageReadStore columnChunkPageReadStore = new ColumnChunkPageReadStore(block.getRowCount());
    for (ColumnChunkMetaData mc : block.getColumns()) {
      String pathKey = Arrays.toString(mc.getPath());
      if (paths.containsKey(pathKey)) {
        ColumnDescriptor columnDescriptor = paths.get(pathKey);
        f.seek(mc.getFirstDataPage());
        if (DEBUG) LOG.debug(f.getPos() + ": start column chunk " + Arrays.toString(mc.getPath()) + " " + mc.getType() + " count=" + mc.getValueCount());
        BytesDecompressor decompressor = codecFactory.getDecompressor(mc.getCodec());
        ColumnChunkPageReader columnChunkPageReader = new ColumnChunkPageReader(decompressor, mc.getValueCount());
        long valuesCountReadSoFar = 0;
        while (valuesCountReadSoFar < mc.getValueCount()) {
          PageHeader pageHeader = readNextDataPageHeader();
          columnChunkPageReader.addPage(
              new Page(
                  BytesInput.copy(BytesInput.from(f, pageHeader.compressed_page_size)),
                  pageHeader.data_page.num_values,
                  pageHeader.uncompressed_page_size
                  ));
          valuesCountReadSoFar += pageHeader.data_page.num_values;
        }
        columnChunkPageReadStore.addColumn(columnDescriptor, columnChunkPageReader);
      }
    }
    ++currentBlock;
    return columnChunkPageReadStore;
  }

  private PageHeader readNextDataPageHeader() throws IOException {
    PageHeader pageHeader;
    do {
      long pos = f.getPos();
      if (DEBUG) LOG.debug(pos + ": reading page");
      try {
        pageHeader = parquetMetadataConverter.readPageHeader(f);
        if (pageHeader.type != PageType.DATA_PAGE) {
          if (DEBUG) LOG.debug("not a data page, skipping " + pageHeader.compressed_page_size);
          f.skip(pageHeader.compressed_page_size);
        }
      } catch (IOException e) {
        throw new IOException("could not read page header at position " + pos, e);
      }
    } while (pageHeader.type != PageType.DATA_PAGE);
    return pageHeader;
  }

  public void close() throws IOException {
    f.close();
    this.codecFactory.release();
  }

}
