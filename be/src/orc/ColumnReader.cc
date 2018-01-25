/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/Int128.hh"

#include "Adaptor.hh"
#include "ByteRLE.hh"
#include "ColumnReader.hh"
#include "Exceptions.hh"
#include "RLE.hh"

#include <math.h>
#include <iostream>

namespace orc {

StripeStreams::~StripeStreams() {
  // PASS
}

inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
  switch (static_cast<int64_t>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:return RleVersion_1;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:return RleVersion_2;
    default:throw ParseError("Unknown encoding in convertRleVersion");
  }
}

ColumnReader::ColumnReader(const Type &type,
                           StripeStreams &stripe
) : columnId(type.getColumnId()),
    memoryPool(stripe.getMemoryPool()) {
  std::unique_ptr<SeekableInputStream> stream =
      stripe.getStream(columnId, proto::Stream_Kind_PRESENT, true);
  if (stream.get()) {
    notNullDecoder = createBooleanRleDecoder(std::move(stream));
  }
}

ColumnReader::~ColumnReader() {
  // PASS
}

uint64_t ColumnReader::skip(uint64_t numValues) {
  ByteRleDecoder *decoder = notNullDecoder.get();
  if (decoder) {
    // page through the values that we want to skip
    // and count how many are non-null
    const size_t MAX_BUFFER_SIZE = 32768;
    size_t bufferSize = std::min(MAX_BUFFER_SIZE,
                                 static_cast<size_t>(numValues));
    char buffer[MAX_BUFFER_SIZE];
    uint64_t remaining = numValues;
    while (remaining > 0) {
      uint64_t chunkSize =
          std::min(remaining,
                   static_cast<uint64_t>(bufferSize));
      decoder->next(buffer, chunkSize, 0);
      remaining -= chunkSize;
      for (uint64_t i = 0; i < chunkSize; ++i) {
        if (!buffer[i]) {
          numValues -= 1;
        }
      }
    }
  }
  return numValues;
}

void ColumnReader::next(ColumnVectorBatch &rowBatch,
                        uint64_t numValues,
                        char *incomingMask) {
  if (numValues > rowBatch.capacity) {
    rowBatch.resize(numValues);
  }
  rowBatch.numElements = numValues;
  ByteRleDecoder *decoder = notNullDecoder.get();
  if (decoder) {
    char *notNullArray = rowBatch.notNull.data();
    decoder->next(notNullArray, numValues, incomingMask);
    // check to see if there are nulls in this batch
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNullArray[i]) {
        rowBatch.hasNulls = true;
        return;
      }
    }
  } else if (incomingMask) {
    // If we don't have a notNull stream, copy the incomingMask
    rowBatch.hasNulls = true;
    memcpy(rowBatch.notNull.data(), incomingMask, numValues);
    return;
  }
  rowBatch.hasNulls = false;
}

/**
 * Expand an array of bytes in place to the corresponding array of longs.
 * Has to work backwards so that they data isn't clobbered during the
 * expansion.
 * @param buffer the array of chars and array of longs that need to be
 *        expanded
 * @param numValues the number of bytes to convert to longs
 */
void expandBytesToLongs(int64_t *buffer, uint64_t numValues) {
  for (size_t i = numValues - 1; i < numValues; --i) {
    buffer[i] = reinterpret_cast<char *>(buffer)[i];
  }
}

class BooleanColumnReader : public ColumnReader {
 private:
  std::unique_ptr<orc::ByteRleDecoder> rle;

 public:
  BooleanColumnReader(const Type &type, StripeStreams &stipe);
  ~BooleanColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

BooleanColumnReader::BooleanColumnReader(const Type &type,
                                         StripeStreams &stripe
) : ColumnReader(type, stripe) {
  rle = createBooleanRleDecoder(stripe.getStream(columnId,
                                                 proto::Stream_Kind_DATA,
                                                 true));
}

BooleanColumnReader::~BooleanColumnReader() {
  // PASS
}

uint64_t BooleanColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

void BooleanColumnReader::next(ColumnVectorBatch &rowBatch,
                               uint64_t numValues,
                               char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  // Since the byte rle places the output in a char* instead of long*,
  // we cheat here and use the long* and then expand it in a second pass.
  int64_t *ptr = dynamic_cast<LongVectorBatch &>(rowBatch).data.data();
  rle->next(reinterpret_cast<char *>(ptr),
            numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
  expandBytesToLongs(ptr, numValues);
}

class ByteColumnReader : public ColumnReader {
 private:
  std::unique_ptr<orc::ByteRleDecoder> rle;

 public:
  ByteColumnReader(const Type &type, StripeStreams &stipe);
  ~ByteColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

ByteColumnReader::ByteColumnReader(const Type &type,
                                   StripeStreams &stripe
) : ColumnReader(type, stripe) {
  rle = createByteRleDecoder(stripe.getStream(columnId,
                                              proto::Stream_Kind_DATA,
                                              true));
}

ByteColumnReader::~ByteColumnReader() {
  // PASS
}

uint64_t ByteColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

void ByteColumnReader::next(ColumnVectorBatch &rowBatch,
                            uint64_t numValues,
                            char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  // Since the byte rle places the output in a char* instead of long*,
  // we cheat here and use the long* and then expand it in a second pass.
  int64_t *ptr = dynamic_cast<LongVectorBatch &>(rowBatch).data.data();
  rle->next(reinterpret_cast<char *>(ptr),
            numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
  expandBytesToLongs(ptr, numValues);
}

class IntegerColumnReader : public ColumnReader {
 protected:
  std::unique_ptr<orc::RleDecoder> rle;

 public:
  IntegerColumnReader(const Type &type, StripeStreams &stripe);
  ~IntegerColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

IntegerColumnReader::IntegerColumnReader(const Type &type,
                                         StripeStreams &stripe
) : ColumnReader(type, stripe) {
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  rle = createRleDecoder(stripe.getStream(columnId,
                                          proto::Stream_Kind_DATA,
                                          true),
                         true, vers, memoryPool);
}

IntegerColumnReader::~IntegerColumnReader() {
  // PASS
}

uint64_t IntegerColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

void IntegerColumnReader::next(ColumnVectorBatch &rowBatch,
                               uint64_t numValues,
                               char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  rle->next(dynamic_cast<LongVectorBatch &>(rowBatch).data.data(),
            numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
}

class TimestampColumnReader : public ColumnReader {
 private:
  std::unique_ptr<orc::RleDecoder> secondsRle;
  std::unique_ptr<orc::RleDecoder> nanoRle;
  const Timezone &writerTimezone;
  const int64_t epochOffset;

 public:
  TimestampColumnReader(const Type &type, StripeStreams &stripe);
  ~TimestampColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

TimestampColumnReader::TimestampColumnReader(const Type &type,
                                             StripeStreams &stripe
) : ColumnReader(type, stripe),
    writerTimezone(stripe.getWriterTimezone()),
    epochOffset(writerTimezone.getEpoch()) {
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  secondsRle = createRleDecoder(stripe.getStream(columnId,
                                                 proto::Stream_Kind_DATA,
                                                 true),
                                true, vers, memoryPool);
  nanoRle = createRleDecoder(stripe.getStream(columnId,
                                              proto::Stream_Kind_SECONDARY,
                                              true),
                             false, vers, memoryPool);
}

TimestampColumnReader::~TimestampColumnReader() {
  // PASS
}

uint64_t TimestampColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  secondsRle->skip(numValues);
  nanoRle->skip(numValues);
  return numValues;
}

void TimestampColumnReader::next(ColumnVectorBatch &rowBatch,
                                 uint64_t numValues,
                                 char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
  TimestampVectorBatch &timestampBatch =
      dynamic_cast<TimestampVectorBatch &>(rowBatch);
  int64_t *secsBuffer = timestampBatch.data.data();
  secondsRle->next(secsBuffer, numValues, notNull);
  int64_t *nanoBuffer = timestampBatch.nanoseconds.data();
  nanoRle->next(nanoBuffer, numValues, notNull);

  // Construct the values
  for (uint64_t i = 0; i < numValues; i++) {
    if (notNull == nullptr || notNull[i]) {
      uint64_t zeros = nanoBuffer[i] & 0x7;
      nanoBuffer[i] >>= 3;
      if (zeros != 0) {
        for (uint64_t j = 0; j <= zeros; ++j) {
          nanoBuffer[i] *= 10;
        }
      }
      int64_t writerTime = secsBuffer[i] + epochOffset;
      secsBuffer[i] = writerTime +
          writerTimezone.getVariant(writerTime).gmtOffset;
      if (secsBuffer[i] < 0 && nanoBuffer[i] != 0) {
        secsBuffer[i] -= 1;
      }
    }
  }
}

class DoubleColumnReader : public ColumnReader {
 public:
  DoubleColumnReader(const Type &type, StripeStreams &stripe);
  ~DoubleColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;

 private:
  std::unique_ptr<SeekableInputStream> inputStream;
  TypeKind columnKind;
  const uint64_t bytesPerValue;
  const char *bufferPointer;
  const char *bufferEnd;

  unsigned char readByte() {
    if (bufferPointer == bufferEnd) {
      int length;
      if (!inputStream->Next
          (reinterpret_cast<const void **>(&bufferPointer), &length)) {
        throw ParseError("bad read in DoubleColumnReader::next()");
      }
      bufferEnd = bufferPointer + length;
    }
    return static_cast<unsigned char>(*(bufferPointer++));
  }

  double readDouble() {
    int64_t bits = 0;
    for (uint64_t i = 0; i < 8; i++) {
      bits |= static_cast<int64_t>(readByte()) << (i * 8);
    }
    double *result = reinterpret_cast<double *>(&bits);
    return *result;
  }

  double readFloat() {
    int32_t bits = 0;
    for (uint64_t i = 0; i < 4; i++) {
      bits |= readByte() << (i * 8);
    }
    float *result = reinterpret_cast<float *>(&bits);
    return static_cast<double>(*result);
  }
};

DoubleColumnReader::DoubleColumnReader(const Type &type,
                                       StripeStreams &stripe
) : ColumnReader(type, stripe),
    inputStream
        (stripe.getStream
            (columnId,
             proto::Stream_Kind_DATA,
             true)),
    columnKind(type.getKind()),
    bytesPerValue((type.getKind() ==
        FLOAT) ? 4 : 8),
    bufferPointer(NULL),
    bufferEnd(NULL) {
  // PASS
}

DoubleColumnReader::~DoubleColumnReader() {
  // PASS
}

uint64_t DoubleColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);

  if (static_cast<size_t>(bufferEnd - bufferPointer) >=
      bytesPerValue * numValues) {
    bufferPointer += bytesPerValue * numValues;
  } else {
    inputStream->Skip(static_cast<int>(bytesPerValue * numValues -
        static_cast<size_t>(bufferEnd -
            bufferPointer)));
    bufferEnd = NULL;
    bufferPointer = NULL;
  }

  return numValues;
}

void DoubleColumnReader::next(ColumnVectorBatch &rowBatch,
                              uint64_t numValues,
                              char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  // update the notNull from the parent class
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  double *outArray = dynamic_cast<DoubleVectorBatch &>(rowBatch).data.data();

  if (columnKind == FLOAT) {
    if (notNull) {
      for (size_t i = 0; i < numValues; ++i) {
        if (notNull[i]) {
          outArray[i] = readFloat();
        }
      }
    } else {
      for (size_t i = 0; i < numValues; ++i) {
        outArray[i] = readFloat();
      }
    }
  } else {
    if (notNull) {
      for (size_t i = 0; i < numValues; ++i) {
        if (notNull[i]) {
          outArray[i] = readDouble();
        }
      }
    } else {
      for (size_t i = 0; i < numValues; ++i) {
        outArray[i] = readDouble();
      }
    }
  }
}

void readFully(char *buffer, int64_t bufferSize, SeekableInputStream *stream) {
  int64_t posn = 0;
  while (posn < bufferSize) {
    const void *chunk;
    int length;
    if (!stream->Next(&chunk, &length)) {
      throw ParseError("bad read in readFully");
    }
    memcpy(buffer + posn, chunk, static_cast<size_t>(length));
    posn += length;
  }
}

class StringDictionaryColumnReader : public ColumnReader {
 private:
  DataBuffer<char> dictionaryBlob;
  DataBuffer <int64_t> dictionaryOffset;
  std::unique_ptr<RleDecoder> rle;
  uint64_t dictionaryCount;

 public:
  StringDictionaryColumnReader(const Type &type, StripeStreams &stipe);
  ~StringDictionaryColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

StringDictionaryColumnReader::StringDictionaryColumnReader
    (const Type &type,
     StripeStreams &stripe
    ) : ColumnReader(type, stripe),
        dictionaryBlob(stripe.getMemoryPool()),
        dictionaryOffset(stripe.getMemoryPool()) {
  RleVersion rleVersion = convertRleVersion(stripe.getEncoding(columnId)
                                                .kind());
  dictionaryCount = stripe.getEncoding(columnId).dictionarysize();
  rle = createRleDecoder(stripe.getStream(columnId,
                                          proto::Stream_Kind_DATA,
                                          true),
                         false, rleVersion, memoryPool);
  std::unique_ptr<RleDecoder> lengthDecoder =
      createRleDecoder(stripe.getStream(columnId,
                                        proto::Stream_Kind_LENGTH,
                                        false),
                       false, rleVersion, memoryPool);
  dictionaryOffset.resize(dictionaryCount + 1);
  int64_t *lengthArray = dictionaryOffset.data();
  lengthDecoder->next(lengthArray + 1, dictionaryCount, 0);
  lengthArray[0] = 0;
  for (uint64_t i = 1; i < dictionaryCount + 1; ++i) {
    lengthArray[i] += lengthArray[i - 1];
  }
  int64_t blobSize = lengthArray[dictionaryCount];
  dictionaryBlob.resize(static_cast<uint64_t>(blobSize));
  std::unique_ptr<SeekableInputStream> blobStream =
      stripe.getStream(columnId, proto::Stream_Kind_DICTIONARY_DATA, false);
  readFully(dictionaryBlob.data(), blobSize, blobStream.get());
}

StringDictionaryColumnReader::~StringDictionaryColumnReader() {
  // PASS
}

uint64_t StringDictionaryColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

void StringDictionaryColumnReader::next(ColumnVectorBatch &rowBatch,
                                        uint64_t numValues,
                                        char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  // update the notNull from the parent class
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  StringVectorBatch &byteBatch = dynamic_cast<StringVectorBatch &>(rowBatch);
  char *blob = dictionaryBlob.data();
  int64_t *dictionaryOffsets = dictionaryOffset.data();
  char **outputStarts = byteBatch.data.data();
  int64_t *outputLengths = byteBatch.length.data();
  rle->next(outputLengths, numValues, notNull);
  if (notNull) {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        int64_t entry = outputLengths[i];
        outputStarts[i] = blob + dictionaryOffsets[entry];
        outputLengths[i] = dictionaryOffsets[entry + 1] -
            dictionaryOffsets[entry];
      }
    }
  } else {
    for (uint64_t i = 0; i < numValues; ++i) {
      int64_t entry = outputLengths[i];
      outputStarts[i] = blob + dictionaryOffsets[entry];
      outputLengths[i] = dictionaryOffsets[entry + 1] -
          dictionaryOffsets[entry];
    }
  }
}

class StringDirectColumnReader : public ColumnReader {
 private:
  DataBuffer<char> blobBuffer;
  std::unique_ptr<RleDecoder> lengthRle;
  std::unique_ptr<SeekableInputStream> blobStream;
  const char *lastBuffer;
  size_t lastBufferLength;

  /**
   * Compute the total length of the values.
   * @param lengths the array of lengths
   * @param notNull the array of notNull flags
   * @param numValues the lengths of the arrays
   * @return the total number of bytes for the non-null values
   */
  size_t computeSize(const int64_t *lengths, const char *notNull,
                     uint64_t numValues);

 public:
  StringDirectColumnReader(const Type &type, StripeStreams &stipe);
  ~StringDirectColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

StringDirectColumnReader::StringDirectColumnReader
    (const Type &type,
     StripeStreams &stripe
    ) : ColumnReader(type, stripe),
        blobBuffer(stripe.getMemoryPool()) {
  RleVersion rleVersion = convertRleVersion(stripe.getEncoding(columnId)
                                                .kind());
  lengthRle = createRleDecoder(stripe.getStream(columnId,
                                                proto::Stream_Kind_LENGTH,
                                                true),
                               false, rleVersion, memoryPool);
  blobStream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
  lastBuffer = 0;
  lastBufferLength = 0;
}

StringDirectColumnReader::~StringDirectColumnReader() {
  // PASS
}

uint64_t StringDirectColumnReader::skip(uint64_t numValues) {
  const size_t BUFFER_SIZE = 1024;
  numValues = ColumnReader::skip(numValues);
  int64_t buffer[BUFFER_SIZE];
  uint64_t done = 0;
  size_t totalBytes = 0;
  // read the lengths, so we know haw many bytes to skip
  while (done < numValues) {
    uint64_t step = std::min(BUFFER_SIZE,
                             static_cast<size_t>(numValues - done));
    lengthRle->next(buffer, step, 0);
    totalBytes += computeSize(buffer, 0, step);
    done += step;
  }
  if (totalBytes <= lastBufferLength) {
    // subtract the needed bytes from the ones left over
    lastBufferLength -= totalBytes;
    lastBuffer += totalBytes;
  } else {
    // move the stream forward after accounting for the buffered bytes
    totalBytes -= lastBufferLength;
    blobStream->Skip(static_cast<int>(totalBytes));
    lastBufferLength = 0;
    lastBuffer = 0;
  }
  return numValues;
}

size_t StringDirectColumnReader::computeSize(const int64_t *lengths,
                                             const char *notNull,
                                             uint64_t numValues) {
  size_t totalLength = 0;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        totalLength += static_cast<size_t>(lengths[i]);
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      totalLength += static_cast<size_t>(lengths[i]);
    }
  }
  return totalLength;
}

void StringDirectColumnReader::next(ColumnVectorBatch &rowBatch,
                                    uint64_t numValues,
                                    char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  // update the notNull from the parent class
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  StringVectorBatch &byteBatch = dynamic_cast<StringVectorBatch &>(rowBatch);
  char **startPtr = byteBatch.data.data();
  int64_t *lengthPtr = byteBatch.length.data();

  // read the length vector
  lengthRle->next(lengthPtr, numValues, notNull);

  // figure out the total length of data we need from the blob stream
  const size_t totalLength = computeSize(lengthPtr, notNull, numValues);

  // Load data from the blob stream into our buffer until we have enough
  // to get the rest directly out of the stream's buffer.
  size_t bytesBuffered = 0;
  blobBuffer.resize(totalLength);
  char *ptr = blobBuffer.data();
  while (bytesBuffered + lastBufferLength < totalLength) {
    blobBuffer.resize(bytesBuffered + lastBufferLength);
    memcpy(ptr + bytesBuffered, lastBuffer, lastBufferLength);
    bytesBuffered += lastBufferLength;
    const void *readBuffer;
    int readLength;
    if (!blobStream->Next(&readBuffer, &readLength)) {
      throw ParseError("failed to read in StringDirectColumnReader.next");
    }
    lastBuffer = static_cast<const char *>(readBuffer);
    lastBufferLength = static_cast<size_t>(readLength);
  }

  // Set up the start pointers for the ones that will come out of the buffer.
  size_t filledSlots = 0;
  size_t usedBytes = 0;
  ptr = blobBuffer.data();
  if (notNull) {
    while (filledSlots < numValues &&
        (!notNull[filledSlots] ||
            usedBytes + static_cast<size_t>(lengthPtr[filledSlots]) <=
                bytesBuffered)) {
      if (notNull[filledSlots]) {
        startPtr[filledSlots] = ptr + usedBytes;
        usedBytes += static_cast<size_t>(lengthPtr[filledSlots]);
      }
      filledSlots += 1;
    }
  } else {
    while (filledSlots < numValues &&
        (usedBytes + static_cast<size_t>(lengthPtr[filledSlots]) <=
            bytesBuffered)) {
      startPtr[filledSlots] = ptr + usedBytes;
      usedBytes += static_cast<size_t>(lengthPtr[filledSlots]);
      filledSlots += 1;
    }
  }

  // do we need to complete the last value in the blob buffer?
  if (usedBytes < bytesBuffered) {
    size_t moreBytes = static_cast<size_t>(lengthPtr[filledSlots]) -
        (bytesBuffered - usedBytes);
    blobBuffer.resize(bytesBuffered + moreBytes);
    ptr = blobBuffer.data();
    memcpy(ptr + bytesBuffered, lastBuffer, moreBytes);
    lastBuffer += moreBytes;
    lastBufferLength -= moreBytes;
    startPtr[filledSlots++] = ptr + usedBytes;
  }

  // Finally, set up any remaining entries into the stream buffer
  if (notNull) {
    while (filledSlots < numValues) {
      if (notNull[filledSlots]) {
        startPtr[filledSlots] = const_cast<char *>(lastBuffer);
        lastBuffer += lengthPtr[filledSlots];
        lastBufferLength -= static_cast<size_t>(lengthPtr[filledSlots]);
      }
      filledSlots += 1;
    }
  } else {
    while (filledSlots < numValues) {
      startPtr[filledSlots] = const_cast<char *>(lastBuffer);
      lastBuffer += lengthPtr[filledSlots];
      lastBufferLength -= static_cast<size_t>(lengthPtr[filledSlots]);
      filledSlots += 1;
    }
  }
}

class StructColumnReader : public ColumnReader {
 private:
  std::vector<ColumnReader *> children;

 public:
  StructColumnReader(const Type &type, StripeStreams &stipe);
  ~StructColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

StructColumnReader::StructColumnReader(const Type &type,
                                       StripeStreams &stripe
) : ColumnReader(type, stripe) {
  // count the number of selected sub-columns
  const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
  switch (static_cast<int64_t>(stripe.getEncoding(columnId).kind())) {
    case proto::ColumnEncoding_Kind_DIRECT:
      for (unsigned int i = 0; i < type.getSubtypeCount(); ++i) {
        const Type &child = *type.getSubtype(i);
        if (selectedColumns[static_cast<uint64_t>(child.getColumnId())]) {
          children.push_back(buildReader(child, stripe).release());
        }
      }
      break;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
    default:throw ParseError("Unknown encoding for StructColumnReader");
  }
}

StructColumnReader::~StructColumnReader() {
  for (size_t i = 0; i < children.size(); i++) {
    delete children[i];
  }
}

uint64_t StructColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  for (std::vector<ColumnReader *>::iterator ptr = children.begin(); ptr != children.end(); ++ptr) {
    (*ptr)->skip(numValues);
  }
  return numValues;
}

void StructColumnReader::next(ColumnVectorBatch &rowBatch,
                              uint64_t numValues,
                              char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  uint64_t i = 0;
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  for (std::vector<ColumnReader *>::iterator ptr = children.begin();
       ptr != children.end(); ++ptr, ++i) {
    (*ptr)->next(*(dynamic_cast<StructVectorBatch &>(rowBatch).fields[i]),
                 numValues, notNull);
  }
}

class ListColumnReader : public ColumnReader {
 private:
  std::unique_ptr<ColumnReader> child;
  std::unique_ptr<RleDecoder> rle;

 public:
  ListColumnReader(const Type &type, StripeStreams &stipe);
  ~ListColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

ListColumnReader::ListColumnReader(const Type &type,
                                   StripeStreams &stripe
) : ColumnReader(type, stripe) {
  // count the number of selected sub-columns
  const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  rle = createRleDecoder(stripe.getStream(columnId,
                                          proto::Stream_Kind_LENGTH,
                                          true),
                         false, vers, memoryPool);
  const Type &childType = *type.getSubtype(0);
  if (selectedColumns[static_cast<uint64_t>(childType.getColumnId())]) {
    child = buildReader(childType, stripe);
  }
}

ListColumnReader::~ListColumnReader() {
  // PASS
}

uint64_t ListColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  ColumnReader *childReader = child.get();
  if (childReader) {
    const uint64_t BUFFER_SIZE = 1024;
    int64_t buffer[BUFFER_SIZE];
    uint64_t childrenElements = 0;
    uint64_t lengthsRead = 0;
    while (lengthsRead < numValues) {
      uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
      rle->next(buffer, chunk, 0);
      for (size_t i = 0; i < chunk; ++i) {
        childrenElements += static_cast<size_t>(buffer[i]);
      }
      lengthsRead += chunk;
    }
    childReader->skip(childrenElements);
  } else {
    rle->skip(numValues);
  }
  return numValues;
}

void ListColumnReader::next(ColumnVectorBatch &rowBatch,
                            uint64_t numValues,
                            char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  ListVectorBatch &listBatch = dynamic_cast<ListVectorBatch &>(rowBatch);
  int64_t *offsets = listBatch.offsets.data();
  notNull = listBatch.hasNulls ? listBatch.notNull.data() : 0;
  rle->next(offsets, numValues, notNull);
  uint64_t totalChildren = 0;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        uint64_t tmp = static_cast<uint64_t>(offsets[i]);
        offsets[i] = static_cast<int64_t>(totalChildren);
        totalChildren += tmp;
      } else {
        offsets[i] = static_cast<int64_t>(totalChildren);
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      uint64_t tmp = static_cast<uint64_t>(offsets[i]);
      offsets[i] = static_cast<int64_t>(totalChildren);
      totalChildren += tmp;
    }
  }
  offsets[numValues] = static_cast<int64_t>(totalChildren);
  ColumnReader *childReader = child.get();
  if (childReader) {
    childReader->next(*(listBatch.elements.get()), totalChildren, 0);
  }
}

class MapColumnReader : public ColumnReader {
 private:
  std::unique_ptr<ColumnReader> keyReader;
  std::unique_ptr<ColumnReader> elementReader;
  std::unique_ptr<RleDecoder> rle;

 public:
  MapColumnReader(const Type &type, StripeStreams &stipe);
  ~MapColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

MapColumnReader::MapColumnReader(const Type &type,
                                 StripeStreams &stripe
) : ColumnReader(type, stripe) {
  // Determine if the key and/or value columns are selected
  const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  rle = createRleDecoder(stripe.getStream(columnId,
                                          proto::Stream_Kind_LENGTH,
                                          true),
                         false, vers, memoryPool);
  const Type &keyType = *type.getSubtype(0);
  if (selectedColumns[static_cast<uint64_t>(keyType.getColumnId())]) {
    keyReader = buildReader(keyType, stripe);
  }
  const Type &elementType = *type.getSubtype(1);
  if (selectedColumns[static_cast<uint64_t>(elementType.getColumnId())]) {
    elementReader = buildReader(elementType, stripe);
  }
}

MapColumnReader::~MapColumnReader() {
  // PASS
}

uint64_t MapColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  ColumnReader *rawKeyReader = keyReader.get();
  ColumnReader *rawElementReader = elementReader.get();
  if (rawKeyReader || rawElementReader) {
    const uint64_t BUFFER_SIZE = 1024;
    int64_t buffer[BUFFER_SIZE];
    uint64_t childrenElements = 0;
    uint64_t lengthsRead = 0;
    while (lengthsRead < numValues) {
      uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
      rle->next(buffer, chunk, 0);
      for (size_t i = 0; i < chunk; ++i) {
        childrenElements += static_cast<size_t>(buffer[i]);
      }
      lengthsRead += chunk;
    }
    if (rawKeyReader) {
      rawKeyReader->skip(childrenElements);
    }
    if (rawElementReader) {
      rawElementReader->skip(childrenElements);
    }
  } else {
    rle->skip(numValues);
  }
  return numValues;
}

void MapColumnReader::next(ColumnVectorBatch &rowBatch,
                           uint64_t numValues,
                           char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  MapVectorBatch &mapBatch = dynamic_cast<MapVectorBatch &>(rowBatch);
  int64_t *offsets = mapBatch.offsets.data();
  notNull = mapBatch.hasNulls ? mapBatch.notNull.data() : 0;
  rle->next(offsets, numValues, notNull);
  uint64_t totalChildren = 0;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        uint64_t tmp = static_cast<uint64_t>(offsets[i]);
        offsets[i] = static_cast<int64_t>(totalChildren);
        totalChildren += tmp;
      } else {
        offsets[i] = static_cast<int64_t>(totalChildren);
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      uint64_t tmp = static_cast<uint64_t>(offsets[i]);
      offsets[i] = static_cast<int64_t>(totalChildren);
      totalChildren += tmp;
    }
  }
  offsets[numValues] = static_cast<int64_t>(totalChildren);
  ColumnReader *rawKeyReader = keyReader.get();
  if (rawKeyReader) {
    rawKeyReader->next(*(mapBatch.keys.get()), totalChildren, 0);
  }
  ColumnReader *rawElementReader = elementReader.get();
  if (rawElementReader) {
    rawElementReader->next(*(mapBatch.elements.get()), totalChildren, 0);
  }
}

class UnionColumnReader : public ColumnReader {
 private:
  std::unique_ptr<ByteRleDecoder> rle;
  std::vector<ColumnReader *> childrenReader;
  std::vector<int64_t> childrenCounts;
  uint64_t numChildren;

 public:
  UnionColumnReader(const Type &type, StripeStreams &stipe);
  ~UnionColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

UnionColumnReader::UnionColumnReader(const Type &type,
                                     StripeStreams &stripe
) : ColumnReader(type, stripe) {
  numChildren = type.getSubtypeCount();
  childrenReader.resize(numChildren);
  childrenCounts.resize(numChildren);

  rle = createByteRleDecoder(stripe.getStream(columnId,
                                              proto::Stream_Kind_DATA,
                                              true));
  // figure out which types are selected
  const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
  for (unsigned int i = 0; i < numChildren; ++i) {
    const Type &child = *type.getSubtype(i);
    if (selectedColumns[static_cast<size_t>(child.getColumnId())]) {
      childrenReader[i] = buildReader(child, stripe).release();
    }
  }
}

UnionColumnReader::~UnionColumnReader() {
  for (std::vector<ColumnReader *>::iterator itr = childrenReader.begin();
       itr != childrenReader.end(); ++itr) {
    delete *itr;
  }
}

uint64_t UnionColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  const uint64_t BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE];
  uint64_t lengthsRead = 0;
  int64_t *counts = childrenCounts.data();
  memset(counts, 0, sizeof(int64_t) * numChildren);
  while (lengthsRead < numValues) {
    uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
    rle->next(buffer, chunk, 0);
    for (size_t i = 0; i < chunk; ++i) {
      counts[static_cast<size_t>(buffer[i])] += 1;
    }
    lengthsRead += chunk;
  }
  for (size_t i = 0; i < numChildren; ++i) {
    if (counts[i] != 0 && childrenReader[i] != NULL) {
      childrenReader[i]->skip(static_cast<uint64_t>(counts[i]));
    }
  }
  return numValues;
}

void UnionColumnReader::next(ColumnVectorBatch &rowBatch,
                             uint64_t numValues,
                             char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  UnionVectorBatch &unionBatch = dynamic_cast<UnionVectorBatch &>(rowBatch);
  uint64_t *offsets = unionBatch.offsets.data();
  int64_t *counts = childrenCounts.data();
  memset(counts, 0, sizeof(int64_t) * numChildren);
  unsigned char *tags = unionBatch.tags.data();
  notNull = unionBatch.hasNulls ? unionBatch.notNull.data() : 0;
  rle->next(reinterpret_cast<char *>(tags), numValues, notNull);
  // set the offsets for each row
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        offsets[i] =
            static_cast<uint64_t>(counts[static_cast<size_t>(tags[i])]++);
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      offsets[i] =
          static_cast<uint64_t>(counts[static_cast<size_t>(tags[i])]++);
    }
  }
  // read the right number of each child column
  for (size_t i = 0; i < numChildren; ++i) {
    if (childrenReader[i] != nullptr) {
      childrenReader[i]->next(*(unionBatch.children[i]),
                              static_cast<uint64_t>(counts[i]), nullptr);
    }
  }
}

/**
 * Destructively convert the number from zigzag encoding to the
 * natural signed representation.
 */
void unZigZagInt128(Int128 &value) {
  bool needsNegate = value.getLowBits() & 1;
  value >>= 1;
  if (needsNegate) {
    value.negate();
    value -= 1;
  }
}

class Decimal64ColumnReader : public ColumnReader {
 public:
  static const uint32_t MAX_PRECISION_64 = 18;
  static const uint32_t MAX_PRECISION_128 = 38;
  static const int64_t POWERS_OF_TEN[MAX_PRECISION_64 + 1];

 protected:
  std::unique_ptr<SeekableInputStream> valueStream;
  int32_t precision;
  int32_t scale;
  const char *buffer;
  const char *bufferEnd;

  std::unique_ptr<RleDecoder> scaleDecoder;

  /**
   * Read the valueStream for more bytes.
   */
  void readBuffer() {
    while (buffer == bufferEnd) {
      int length;
      if (!valueStream->Next(reinterpret_cast<const void **>(&buffer),
                             &length)) {
        throw ParseError("Read past end of stream in Decimal64ColumnReader " +
            valueStream->getName());
      }
      bufferEnd = buffer + length;
    }
  }

  void readInt64(int64_t &value, int32_t currentScale) {
    value = 0;
    size_t offset = 0;
    while (true) {
      readBuffer();
      unsigned char ch = static_cast<unsigned char>(*(buffer++));
      value |= static_cast<uint64_t>(ch & 0x7f) << offset;
      offset += 7;
      if (!(ch & 0x80)) {
        break;
      }
    }
    value = unZigZag(static_cast<uint64_t>(value));
    if (scale > currentScale) {
      value *= POWERS_OF_TEN[scale - currentScale];
    } else if (scale < currentScale) {
      value /= POWERS_OF_TEN[currentScale - scale];
    }
  }

 public:
  Decimal64ColumnReader(const Type &type, StripeStreams &stipe);
  ~Decimal64ColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};
const uint32_t Decimal64ColumnReader::MAX_PRECISION_64;
const uint32_t Decimal64ColumnReader::MAX_PRECISION_128;
const int64_t Decimal64ColumnReader::POWERS_OF_TEN[MAX_PRECISION_64 + 1] =
    {1,
     10,
     100,
     1000,
     10000,
     100000,
     1000000,
     10000000,
     100000000,
     1000000000,
     10000000000,
     100000000000,
     1000000000000,
     10000000000000,
     100000000000000,
     1000000000000000,
     10000000000000000,
     100000000000000000,
     1000000000000000000};

Decimal64ColumnReader::Decimal64ColumnReader(const Type &type,
                                             StripeStreams &stripe
) : ColumnReader(type, stripe) {
  scale = static_cast<int32_t>(type.getScale());
  precision = static_cast<int32_t>(type.getPrecision());
  valueStream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
  buffer = nullptr;
  bufferEnd = nullptr;
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  scaleDecoder = createRleDecoder(stripe.getStream
                                      (columnId,
                                       proto::Stream_Kind_SECONDARY,
                                       true),
                                  true, vers, memoryPool);
}

Decimal64ColumnReader::~Decimal64ColumnReader() {
  // PASS
}

uint64_t Decimal64ColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  uint64_t skipped = 0;
  while (skipped < numValues) {
    readBuffer();
    if (!(0x80 & *(buffer++))) {
      skipped += 1;
    }
  }
  scaleDecoder->skip(numValues);
  return numValues;
}

void Decimal64ColumnReader::next(ColumnVectorBatch &rowBatch,
                                 uint64_t numValues,
                                 char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  Decimal64VectorBatch &batch =
      dynamic_cast<Decimal64VectorBatch &>(rowBatch);
  int64_t *values = batch.values.data();
  // read the next group of scales
  int64_t *scaleBuffer = batch.readScales.data();
  scaleDecoder->next(scaleBuffer, numValues, notNull);
  batch.precision = precision;
  batch.scale = scale;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        readInt64(values[i], static_cast<int32_t>(scaleBuffer[i]));
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      readInt64(values[i], static_cast<int32_t>(scaleBuffer[i]));
    }
  }
}

void scaleInt128(Int128 &value, uint32_t scale, uint32_t currentScale) {
  if (scale > currentScale) {
    while (scale > currentScale) {
      uint32_t scaleAdjust =
          std::min(Decimal64ColumnReader::MAX_PRECISION_64,
                   scale - currentScale);
      value *= Decimal64ColumnReader::POWERS_OF_TEN[scaleAdjust];
      currentScale += scaleAdjust;
    }
  } else if (scale < currentScale) {
    Int128 remainder;
    while (currentScale > scale) {
      uint32_t scaleAdjust =
          std::min(Decimal64ColumnReader::MAX_PRECISION_64,
                   currentScale - scale);
      value = value.divide(Decimal64ColumnReader::POWERS_OF_TEN[scaleAdjust],
                           remainder);
      currentScale -= scaleAdjust;
    }
  }
}

class Decimal128ColumnReader : public Decimal64ColumnReader {
 public:
  Decimal128ColumnReader(const Type &type, StripeStreams &stipe);
  ~Decimal128ColumnReader();

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;

 private:
  void readInt128(Int128 &value, int32_t currentScale) {
    value = 0;
    Int128 work;
    uint32_t offset = 0;
    while (true) {
      readBuffer();
      unsigned char ch = static_cast<unsigned char>(*(buffer++));
      work = ch & 0x7f;
      work <<= offset;
      value |= work;
      offset += 7;
      if (!(ch & 0x80)) {
        break;
      }
    }
    unZigZagInt128(value);
    scaleInt128(value, static_cast<uint32_t>(scale),
                static_cast<uint32_t>(currentScale));
  }
};

Decimal128ColumnReader::Decimal128ColumnReader
    (const Type &type,
     StripeStreams &stripe
    ) : Decimal64ColumnReader(type, stripe) {
  // PASS
}

Decimal128ColumnReader::~Decimal128ColumnReader() {
  // PASS
}

void Decimal128ColumnReader::next(ColumnVectorBatch &rowBatch,
                                  uint64_t numValues,
                                  char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  Decimal128VectorBatch &batch =
      dynamic_cast<Decimal128VectorBatch &>(rowBatch);
  Int128 *values = batch.values.data();
  // read the next group of scales
  int64_t *scaleBuffer = batch.readScales.data();
  scaleDecoder->next(scaleBuffer, numValues, notNull);
  batch.precision = precision;
  batch.scale = scale;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]));
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]));
    }
  }
}

class DecimalHive11ColumnReader : public Decimal64ColumnReader {
 private:
  bool throwOnOverflow;
  std::ostream *errorStream;

  /**
   * Read an Int128 from the stream and correct it to the desired scale.
   */
  bool readInt128(Int128 &value, int32_t currentScale) {
    // -/+ 99999999999999999999999999999999999999
    static const Int128 MIN_VALUE(-0x4b3b4ca85a86c47b, 0xf675ddc000000001);
    static const Int128 MAX_VALUE(0x4b3b4ca85a86c47a, 0x098a223fffffffff);

    value = 0;
    Int128 work;
    uint32_t offset = 0;
    bool result = true;
    while (true) {
      readBuffer();
      unsigned char ch = static_cast<unsigned char>(*(buffer++));
      work = ch & 0x7f;
      // If we have read more than 128 bits, we flag the error, but keep
      // reading bytes so the stream isn't thrown off.
      if (offset > 128 || (offset == 126 && work > 3)) {
        result = false;
      }
      work <<= offset;
      value |= work;
      offset += 7;
      if (!(ch & 0x80)) {
        break;
      }
    }

    if (!result) {
      return result;
    }
    unZigZagInt128(value);
    scaleInt128(value, static_cast<uint32_t>(scale),
                static_cast<uint32_t>(currentScale));
    return value >= MIN_VALUE && value <= MAX_VALUE;
  }

 public:
  DecimalHive11ColumnReader(const Type &type, StripeStreams &stipe);
  ~DecimalHive11ColumnReader();

  void next(ColumnVectorBatch &rowBatch,
            uint64_t numValues,
            char *notNull) override;
};

DecimalHive11ColumnReader::DecimalHive11ColumnReader
    (const Type &type,
     StripeStreams &stripe
    ) : Decimal64ColumnReader(type, stripe) {
  const ReaderOptions options = stripe.getReaderOptions();
  scale = options.getForcedScaleOnHive11Decimal();
  throwOnOverflow = options.getThrowOnHive11DecimalOverflow();
  errorStream = options.getErrorStream();
}

DecimalHive11ColumnReader::~DecimalHive11ColumnReader() {
  // PASS
}

void DecimalHive11ColumnReader::next(ColumnVectorBatch &rowBatch,
                                     uint64_t numValues,
                                     char *notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  Decimal128VectorBatch &batch =
      dynamic_cast<Decimal128VectorBatch &>(rowBatch);
  Int128 *values = batch.values.data();
  // read the next group of scales
  int64_t *scaleBuffer = batch.readScales.data();

  scaleDecoder->next(scaleBuffer, numValues, notNull);

  batch.precision = precision;
  batch.scale = scale;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        if (!readInt128(values[i],
                        static_cast<int32_t>(scaleBuffer[i]))) {
          if (throwOnOverflow) {
            throw ParseError("Hive 0.11 decimal was more than 38 digits.");
          } else {
            *errorStream << "Warning: "
                         << "Hive 0.11 decimal with more than 38 digits "
                         << "replaced by NULL.\n";
            notNull[i] = false;
          }
        }
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      if (!readInt128(values[i],
                      static_cast<int32_t>(scaleBuffer[i]))) {
        if (throwOnOverflow) {
          throw ParseError("Hive 0.11 decimal was more than 38 digits.");
        } else {
          *errorStream << "Warning: "
                       << "Hive 0.11 decimal with more than 38 digits "
                       << "replaced by NULL.\n";
          batch.hasNulls = true;
          batch.notNull[i] = false;
        }
      }
    }
  }
}

/**
 * Create a reader for the given stripe.
 */
std::unique_ptr<ColumnReader> buildReader(const Type &type,
                                          StripeStreams &stripe) {
  switch (static_cast<int64_t>(type.getKind())) {
    case DATE:
    case INT:
    case LONG:
    case SHORT:
      return std::unique_ptr<ColumnReader>(
          new IntegerColumnReader(type, stripe));
    case BINARY:
    case CHAR:
    case STRING:
    case VARCHAR:
      switch (static_cast<int64_t>(stripe.getEncoding(type.getColumnId()).kind())) {
        case proto::ColumnEncoding_Kind_DICTIONARY:
        case proto::ColumnEncoding_Kind_DICTIONARY_V2:
          return std::unique_ptr<ColumnReader>(
              new StringDictionaryColumnReader(type, stripe));
        case proto::ColumnEncoding_Kind_DIRECT:
        case proto::ColumnEncoding_Kind_DIRECT_V2:
          return std::unique_ptr<ColumnReader>(
              new StringDirectColumnReader(type, stripe));
        default:throw NotImplementedYet("buildReader unhandled string encoding");
      }

    case BOOLEAN:
      return std::unique_ptr<ColumnReader>(
          new BooleanColumnReader(type, stripe));

    case BYTE:
      return std::unique_ptr<ColumnReader>(
          new ByteColumnReader(type, stripe));

    case LIST:
      return std::unique_ptr<ColumnReader>(
          new ListColumnReader(type, stripe));

    case MAP:
      return std::unique_ptr<ColumnReader>(
          new MapColumnReader(type, stripe));

    case UNION:
      return std::unique_ptr<ColumnReader>(
          new UnionColumnReader(type, stripe));

    case STRUCT:
      return std::unique_ptr<ColumnReader>(
          new StructColumnReader(type, stripe));

    case FLOAT:
    case DOUBLE:
      return std::unique_ptr<ColumnReader>(
          new DoubleColumnReader(type, stripe));

    case TIMESTAMP:
      return std::unique_ptr<ColumnReader>
          (new TimestampColumnReader(type, stripe));

    case DECIMAL:
      // is this a Hive 0.11 or 0.12 file?
      if (type.getPrecision() == 0) {
        return std::unique_ptr<ColumnReader>
            (new DecimalHive11ColumnReader(type, stripe));

        // can we represent the values using int64_t?
      } else if (type.getPrecision() <=
          Decimal64ColumnReader::MAX_PRECISION_64) {
        return std::unique_ptr<ColumnReader>
            (new Decimal64ColumnReader(type, stripe));

        // otherwise we use the Int128 implementation
      } else {
        return std::unique_ptr<ColumnReader>
            (new Decimal128ColumnReader(type, stripe));
      }

    default:throw NotImplementedYet("buildReader unhandled type");
  }
}

}
