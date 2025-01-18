package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.local.io.util.PinotByteBufferBitSet;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.local.io.writer.impl.FrameOfReferenceChunkFwdIndexWriter.*;


public class FrameOfReferenceChunkFwdIndexReader implements ForwardIndexReader<ChunkReaderContext> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrameOfReferenceChunkFwdIndexReader.class);

  protected final PinotDataBuffer _dataBuffer;
  protected final FieldSpec.DataType _storedType;

  // Header information.
  protected final int _numChunks;
  protected final int _numDocsPerChunk;
  protected final int _sizeOfEntryInBits;

  protected final boolean _isCompressed;
  protected final ChunkCompressionType _compressionType;
  protected final ChunkDecompressor _chunkDecompressor;

  private final long _minValue;

  private final int _chunkSize;
  protected final int _headerEntryChunkOffsetSize;
  protected final int _dataHeaderStart;

  protected final PinotDataBuffer _dataHeader;

  protected final int _rawDataStart;
  protected final PinotDataBuffer _rawData;
  protected final PinotDataBitSet _rawDataBitSet;


  public FrameOfReferenceChunkFwdIndexReader(PinotDataBuffer dataBuffer, FieldSpec.DataType valueType) {
    _dataBuffer = dataBuffer;
    _storedType = valueType;
    /**
     *     int offset = 0;
     *     _header.put(MAGIC_BYTES);
     *     offset += MAGIC_BYTES.length;
     *
     *     _header.putInt(numChunks);
     *     offset += Integer.BYTES;
     *
     *     _header.putInt(numDocsPerChunk);
     *     offset += Integer.BYTES;
     *
     *     _header.putInt(_sizeOfEntryInBits);
     *     offset += Integer.BYTES;
     *
     *     // Write total number of docs.
     *     _header.putInt(totalDocs);
     *     offset += Integer.BYTES;
     *
     *     // Write the compressor type
     *     _header.putInt(compressionType.getValue());
     *     offset += Integer.BYTES;
     *
     *     _header.putLong(minValue);
     *     offset += Long.BYTES;
     *
     *     // Start of chunk offsets.
     *     int dataHeaderStart = offset + Integer.BYTES;
     *     _header.putInt(dataHeaderStart);
     */

    // Start header after magic bytes.
    int headerOffset = MAGIC_BYTES.length;
    _numChunks = dataBuffer.getInt(headerOffset);
    headerOffset += Integer.BYTES;

    _numDocsPerChunk = _dataBuffer.getInt(headerOffset);
    headerOffset += Integer.BYTES;

    _sizeOfEntryInBits = _dataBuffer.getInt(headerOffset);
    headerOffset += Integer.BYTES;

    _dataBuffer.getInt(headerOffset); // Total docs
    headerOffset += Integer.BYTES;

    _compressionType = ChunkCompressionType.valueOf(_dataBuffer.getInt(headerOffset));
    _chunkDecompressor = ChunkCompressorFactory.getDecompressor(_compressionType);
    _isCompressed = !_compressionType.equals(ChunkCompressionType.PASS_THROUGH);
    headerOffset += Integer.BYTES;

    _minValue = _dataBuffer.getLong(headerOffset);
    headerOffset += Long.BYTES;

    _dataHeaderStart = _dataBuffer.getInt(headerOffset);
    _headerEntryChunkOffsetSize = Long.BYTES;
    int dataHeaderLength = _numChunks * _headerEntryChunkOffsetSize;
    int rawDataStart = _dataHeaderStart + dataHeaderLength;
    _dataHeader = _dataBuffer.view(_dataHeaderStart, rawDataStart);

   // Useful for uncompressed data.
    _rawDataStart = rawDataStart;
    _rawData = _dataBuffer.view(rawDataStart, _dataBuffer.size());
    _rawDataBitSet = new PinotDataBitSet(_rawData);


    _chunkSize = (_numDocsPerChunk * _sizeOfEntryInBits + Byte.SIZE - 1) / Byte.SIZE;

    // TODO(Vivek): Remove.
    LOGGER.info("Header: numChunks: {}, numDocsPerChunk: {}, lengthOfEntryInBits: {}, compressionType: {}, minValue: {}, dataHeaderStart: {}",
        _numChunks, _numDocsPerChunk, _sizeOfEntryInBits, _compressionType, _minValue, _dataHeaderStart);
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
  public FieldSpec.DataType getStoredType() {
    return _storedType;
  }

  @Override
  public ChunkCompressionType getCompressionType() {
    return _compressionType;
  }

  @Override
  public int getLengthOfLongestEntry() {
    // TODO(Vivek): This is in bytes. Check if it is really needed.
    return -1;
  }


  @Nullable
  @Override
  public ChunkReaderContext createContext() {
    return new ChunkReaderContext(_chunkSize);
  }

  @Override
  public int getInt(int docId, ChunkReaderContext context) {
//    if (_isCompressed) {
      int chunkRowId = docId % _numDocsPerChunk;
      ByteBuffer chunkBuffer = getChunkBuffer(docId, context);
      PinotByteBufferBitSet pinotByteBufferBitSet = new PinotByteBufferBitSet(chunkBuffer);
      int finalVal = pinotByteBufferBitSet.readInt(chunkRowId, _sizeOfEntryInBits) + (int) _minValue;
      return finalVal;
//    } else {
//
//      // TODO(Vivek): Implement it for passthrough
//      return _rawDataBitSet.readInt(docId, _sizeOfEntryInBits);
//    }
  }

  public long getLong(int docId, ChunkReaderContext context) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public float getFloat(int docId, ChunkReaderContext context) {
    throw new UnsupportedOperationException("Not implemented");

  }

  @Override
  public double getDouble(int docId, ChunkReaderContext context) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void readValuesSV(int[] docIds, int length, int[] values, ChunkReaderContext context) {
    if (_storedType.isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (_storedType) {
        case INT: {
          int minOffset = docIds[0] * _sizeOfEntryInBits;
          _rawDataBitSet.readInt(minOffset, _sizeOfEntryInBits, docIds.length, values);
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      ForwardIndexReader.super.readValuesSV(docIds, length, values, context);
    }
  }

  @Override
  public void readValuesSV(int[] docIds, int length, long[] values, ChunkReaderContext context) {
    if (_storedType.isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (_storedType) {
        case INT: {
          int minOffset = docIds[0] * _sizeOfEntryInBits;
          _rawDataBitSet.readInt(minOffset, _sizeOfEntryInBits, docIds.length, values);
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      ForwardIndexReader.super.readValuesSV(docIds, length, values, context);
    }
  }

  @Override
  public void readValuesSV(int[] docIds, int length, float[] values, ChunkReaderContext context) {
    if (_storedType.isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (_storedType) {
        case INT: {
          int minOffset = docIds[0] * _sizeOfEntryInBits;
          _rawDataBitSet.readInt(minOffset, _sizeOfEntryInBits, docIds.length, values);
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      ForwardIndexReader.super.readValuesSV(docIds, length, values, context);
    }
  }

  @Override
  public void readValuesSV(int[] docIds, int length, double[] values, ChunkReaderContext context) {
    if (_storedType.isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (_storedType) {
        case INT: {
          int minOffset = docIds[0] * _sizeOfEntryInBits;
          _rawDataBitSet.readInt(minOffset, _sizeOfEntryInBits, docIds.length, values);
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      ForwardIndexReader.super.readValuesSV(docIds, length, values, context);
    }
  }



  @Override
  public boolean isBufferByteRangeInfoSupported() {
    // TODO(Vivek): Not sure what to return here. Investigate.
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void recordDocIdByteRanges(int docId, ChunkReaderContext context, @Nullable List<ByteRange> ranges) {
    // If uncompressed, should use fixed offset
    throw new UnsupportedOperationException("Forward index is fixed length type");
  }

  @Override
  public boolean isFixedOffsetMappingType() {
    throw new UnsupportedOperationException("Forward index is fixed length type");
  }

  @Override
  public long getRawDataStartOffset() {
    throw new UnsupportedOperationException("Forward index is not fixed length type");
  }

  @Override
  public int getDocLength() {
    if (isFixedOffsetMappingType()) {
      return _storedType.size();
    }
    throw new UnsupportedOperationException("Forward index is not fixed length type");
  }

  protected int getChunkId(int docId) {
    return docId / _numDocsPerChunk;
  }

  protected long getChunkPosition(int chunkId) {
    return _dataHeader.getLong(chunkId * _headerEntryChunkOffsetSize);
  }

  protected ByteBuffer getChunkBuffer(int docId, ChunkReaderContext context) {
    int chunkId = getChunkId(docId);
    if (context.getChunkId() == chunkId) {
      return context.getChunkBuffer();
    }
    return decompressChunk(chunkId, context);
  }

  protected ByteBuffer decompressChunk(int chunkId, ChunkReaderContext context) {
    int chunkSize;
    long chunkPosition = getChunkPosition(chunkId);

    // Size of chunk can be determined using next chunks offset, or end of data buffer for last chunk.
    if (chunkId == (_numChunks - 1)) { // Last chunk.
      chunkSize = (int) (_dataBuffer.size() - chunkPosition);
    } else {
      long nextChunkOffset = getChunkPosition(chunkId + 1);
      chunkSize = (int) (nextChunkOffset - chunkPosition);
    }

    ByteBuffer decompressedBuffer = context.getChunkBuffer();
    decompressedBuffer.clear();

    try {
      _chunkDecompressor.decompress(_dataBuffer.toDirectByteBuffer(chunkPosition, chunkSize), decompressedBuffer);
    } catch (IOException e) {
      LOGGER.error("Exception caught while decompressing data chunk", e);
      throw new RuntimeException(e);
    }
    context.setChunkId(chunkId);
    return decompressedBuffer;
  }

  @Override
  public void close()
      throws IOException {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
    _chunkDecompressor.close();
  }

  private boolean isContiguousRange(int[] docIds, int length) {
    return docIds[length - 1] - docIds[0] == length - 1;
  }
}
