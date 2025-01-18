package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import java.io.File;
import java.io.IOException;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.FrameOfReferenceChunkFwdIndexWriter;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


public class FORForwardIndexCreator implements ForwardIndexCreator {
  private final FrameOfReferenceChunkFwdIndexWriter _indexWriter;
  private final FieldSpec.DataType _valueType;

  /**
   * Constructor for the class
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param valueType Type of the values
   * @throws IOException
   */
  public FORForwardIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, FieldSpec.DataType valueType)
      throws IOException {
    this(baseIndexDir, compressionType, column, totalDocs, valueType, ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION,
        ForwardIndexConfig.DEFAULT_TARGET_DOCS_PER_CHUNK);
  }

  /**
   * Constructor for the class
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param valueType Type of the values
   * @param writerVersion writer format version
   * @throws IOException
   */
  public FORForwardIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, FieldSpec.DataType valueType, int writerVersion, int targetDocsPerChunk)
      throws IOException {
    File file = new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);

    _indexWriter = new FrameOfReferenceChunkFwdIndexWriter(file, compressionType, totalDocs, targetDocsPerChunk,2147483639, 2147483647);

    _valueType = valueType;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _valueType;
  }

  @Override
  public void putInt(int value) {
    _indexWriter.putInt(value);
  }

  @Override
  public void putLong(long value) {
    _indexWriter.putLong(value);
  }

  @Override
  public void putFloat(float value) {
    // Not implemented
  }

  @Override
  public void putDouble(double value) {
    // Not implemented
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }
}
