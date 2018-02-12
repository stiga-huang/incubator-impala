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

#ifndef ORC_READER_HH
#define ORC_READER_HH

#include "orc-config.hh"
#include "Statistics.hh"
#include "Type.hh"
#include "Vector.hh"

#include <memory>
#include <string>
#include <vector>

namespace orc {

  // classes that hold data members so we can maintain binary compatibility
  struct ReaderOptionsPrivate;
  struct RowReaderOptionsPrivate;

  enum CompressionKind {
    CompressionKind_NONE = 0,
    CompressionKind_ZLIB = 1,
    CompressionKind_SNAPPY = 2,
    CompressionKind_LZO = 3,
    CompressionKind_LZ4 = 4,
    CompressionKind_ZSTD = 5,
    CompressionKind_MAX = INT64_MAX
  };

  /**
   * Get the name of the CompressionKind.
   */
  std::string compressionKindToString(CompressionKind kind);

  enum WriterVersion {
    WriterVersion_ORIGINAL = 0,
    WriterVersion_HIVE_8732 = 1,
    WriterVersion_HIVE_4243 = 2,
    WriterVersion_HIVE_12055 = 3,
    WriterVersion_HIVE_13083 = 4,
    WriterVersion_ORC_101 = 5,
    WriterVersion_ORC_135 = 6,
    WriterVersion_MAX = INT64_MAX
  };

  /**
   * Get the name of the WriterVersion.
   */
  std::string writerVersionToString(WriterVersion kind);

  enum StreamKind {
    StreamKind_PRESENT = 0,
    StreamKind_DATA = 1,
    StreamKind_LENGTH = 2,
    StreamKind_DICTIONARY_DATA = 3,
    StreamKind_DICTIONARY_COUNT = 4,
    StreamKind_SECONDARY = 5,
    StreamKind_ROW_INDEX = 6,
    StreamKind_BLOOM_FILTER = 7
  };

  /**
   * Get the string representation of the StreamKind.
   */
  std::string streamKindToString(StreamKind kind);

  class StreamInformation {
  public:
    virtual ~StreamInformation();

    virtual StreamKind getKind() const = 0;
    virtual uint64_t getColumnId() const = 0;
    virtual uint64_t getOffset() const = 0;
    virtual uint64_t getLength() const = 0;
  };

  enum ColumnEncodingKind {
    ColumnEncodingKind_DIRECT = 0,
    ColumnEncodingKind_DICTIONARY = 1,
    ColumnEncodingKind_DIRECT_V2 = 2,
    ColumnEncodingKind_DICTIONARY_V2 = 3
  };

  std::string columnEncodingKindToString(ColumnEncodingKind kind);

  class StripeInformation {
  public:
    virtual ~StripeInformation();

    /**
     * Get the byte offset of the start of the stripe.
     * @return the bytes from the start of the file
     */
    virtual uint64_t getOffset() const = 0;

    /**
     * Get the total length of the stripe in bytes.
     * @return the number of bytes in the stripe
     */
    virtual uint64_t getLength() const = 0;

    /**
     * Get the length of the stripe's indexes.
     * @return the number of bytes in the index
     */
    virtual uint64_t getIndexLength() const = 0;

    /**
     * Get the length of the stripe's data.
     * @return the number of bytes in the stripe
     */
    virtual uint64_t getDataLength()const = 0;

    /**
     * Get the length of the stripe's tail section, which contains its index.
     * @return the number of bytes in the tail
     */
    virtual uint64_t getFooterLength() const = 0;

    /**
     * Get the number of rows in the stripe.
     * @return a count of the number of rows
     */
    virtual uint64_t getNumberOfRows() const = 0;

    /**
     * Get the number of streams in the stripe.
     */
    virtual uint64_t getNumberOfStreams() const = 0;

    /**
     * Get the StreamInformation for the given stream.
     */
    virtual ORC_UNIQUE_PTR<StreamInformation>
      getStreamInformation(uint64_t streamId) const = 0;

    /**
     * Get the column encoding for the given column.
     * @param colId the columnId
     */
    virtual ColumnEncodingKind getColumnEncoding(uint64_t colId) const = 0;

    /**
     * Get the dictionary size.
     * @param colId the columnId
     * @return the size of the dictionary or 0 if there isn't one
     */
    virtual uint64_t getDictionarySize(uint64_t colId) const = 0;

    /**
     * Get the writer timezone.
     */
    virtual const std::string& getWriterTimezone() const = 0;
  };

  /**
   * Options for creating a Reader.
   */
  class ReaderOptions {
  private:
    ORC_UNIQUE_PTR<ReaderOptionsPrivate> privateBits;

  public:
    ReaderOptions();
    ReaderOptions(const ReaderOptions&);
    ReaderOptions(ReaderOptions&);
    ReaderOptions& operator=(const ReaderOptions&);
    virtual ~ReaderOptions();

    /**
     * Set the stream to use for printing warning or error messages.
     */
    ReaderOptions& setErrorStream(std::ostream& stream);

    /**
     * Open the file used a serialized copy of the file tail.
     *
     * When one process opens the file and other processes need to read
     * the rows, we want to enable clients to just read the tail once.
     * By passing the string returned by Reader.getSerializedFileTail(), to
     * this function, the second reader will not need to read the file tail
     * from disk.
     *
     * @param serialization the bytes of the serialized tail to use
     */
    ReaderOptions& setSerializedFileTail(const std::string& serialization);

    /**
     * Set the memory allocator.
     */
    ReaderOptions& setMemoryPool(MemoryPool& pool);

    /**
     * Set the location of the tail as defined by the logical length of the
     * file.
     */
    ReaderOptions& setTailLocation(uint64_t offset);

    /**
     * Get the stream to write warnings or errors to.
     */
    std::ostream* getErrorStream() const;

    /**
     * Get the serialized file tail that the user passed in.
     */
    std::string getSerializedFileTail() const;

    /**
     * Get the desired tail location.
     * @return if not set, return the maximum long.
     */
    uint64_t getTailLocation() const;

    /**
     * Get the memory allocator.
     */
    MemoryPool* getMemoryPool() const;
  };

  /**
   * Options for creating a RowReader.
   */
  class RowReaderOptions {
  private:
    ORC_UNIQUE_PTR<RowReaderOptionsPrivate> privateBits;

  public:
    RowReaderOptions();
    RowReaderOptions(const RowReaderOptions&);
    RowReaderOptions(RowReaderOptions&);
    RowReaderOptions& operator=(const RowReaderOptions&);
    virtual ~RowReaderOptions();

    /**
     * For files that have structs as the top-level object, select the fields
     * to read. The first field is 0, the second 1, and so on. By default,
     * all columns are read. This option clears any previous setting of
     * the selected columns.
     * @param include a list of fields to read
     * @return this
     */
    RowReaderOptions& include(const std::list<uint64_t>& include);

    /**
     * For files that have structs as the top-level object, select the fields
     * to read by name. By default, all columns are read. This option clears
     * any previous setting of the selected columns.
     * @param include a list of fields to read
     * @return this
     */
    RowReaderOptions& include(const std::list<std::string>& include);

    /**
     * Selects which type ids to read. The root type is always 0 and the
     * rest of the types are labeled in a preorder traversal of the tree.
     * The parent types are automatically selected, but the children are not.
     *
     * This option clears any previous setting of the selected columns or
     * types.
     * @param types a list of the type ids to read
     * @return this
     */
    RowReaderOptions& includeTypes(const std::list<uint64_t>& types);

    /**
     * Set the section of the file to process.
     * @param offset the starting byte offset
     * @param length the number of bytes to read
     * @return this
     */
    RowReaderOptions& range(uint64_t offset, uint64_t length);

    /**
     * For Hive 0.11 (and 0.12) decimals, the precision was unlimited
     * and thus may overflow the 38 digits that is supported. If one
     * of the Hive 0.11 decimals is too large, the reader may either convert
     * the value to NULL or throw an exception. That choice is controlled
     * by this setting.
     *
     * Defaults to true.
     *
     * @param shouldThrow should the reader throw a ParseError?
     * @return returns *this
     */
    RowReaderOptions& throwOnHive11DecimalOverflow(bool shouldThrow);

    /**
     * For Hive 0.11 (and 0.12) written decimals, which have unlimited
     * scale and precision, the reader forces the scale to a consistent
     * number that is configured. This setting changes the scale that is
     * forced upon these old decimals. See also throwOnHive11DecimalOverflow.
     *
     * Defaults to 6.
     *
     * @param forcedScale the scale that will be forced on Hive 0.11 decimals
     * @return returns *this
     */
    RowReaderOptions& forcedScaleOnHive11Decimal(int32_t forcedScale);

    /**
     * Were the field ids set?
     */
    bool getIndexesSet() const;

    /**
     * Were the type ids set?
     */
    bool getTypeIdsSet() const;

    /**
     * Get the list of selected field or type ids to read.
     */
    const std::list<uint64_t>& getInclude() const;

    /**
     * Were the include names set?
     */
    bool getNamesSet() const;

    /**
     * Get the list of selected columns to read. All children of the selected
     * columns are also selected.
     */
    const std::list<std::string>& getIncludeNames() const;

    /**
     * Get the start of the range for the data being processed.
     * @return if not set, return 0
     */
    uint64_t getOffset() const;

    /**
     * Get the end of the range for the data being processed.
     * @return if not set, return the maximum long
     */
    uint64_t getLength() const;

    /**
     * Should the reader throw a ParseError when a Hive 0.11 decimal is
     * larger than the supported 38 digits of precision? Otherwise, the
     * data item is replaced by a NULL.
     */
    bool getThrowOnHive11DecimalOverflow() const;

    /**
     * What scale should all Hive 0.11 decimals be normalized to?
     */
    int32_t getForcedScaleOnHive11Decimal() const;
  };


  class RowReader;

  /**
   * The interface for reading ORC file meta-data and constructing RowReaders.
   * This is an an abstract class that will subclassed as necessary.
   */
  class Reader {
  public:
    virtual ~Reader();

    /**
     * Get the format version of the file. Currently known values are:
     * "0.11" and "0.12"
     * @return the version string
     */
    virtual std::string getFormatVersion() const = 0;

    /**
     * Get the number of rows in the file.
     * @return the number of rows
     */
    virtual uint64_t getNumberOfRows() const = 0;

    /**
     * Get the user metadata keys.
     * @return the set of metadata keys
     */
    virtual std::list<std::string> getMetadataKeys() const = 0;

    /**
     * Get a user metadata value.
     * @param key a key given by the user
     * @return the bytes associated with the given key
     */
    virtual std::string getMetadataValue(const std::string& key) const = 0;

    /**
     * Did the user set the given metadata value.
     * @param key the key to check
     * @return true if the metadata value was set
     */
    virtual bool hasMetadataValue(const std::string& key) const = 0;

    /**
     * Get the compression kind.
     * @return the kind of compression in the file
     */
    virtual CompressionKind getCompression() const = 0;

    /**
     * Get the buffer size for the compression.
     * @return number of bytes to buffer for the compression codec.
     */
    virtual uint64_t getCompressionSize() const = 0;

    /**
     * Get the version of the writer.
     * @return the version of the writer.
     */
    virtual WriterVersion getWriterVersion() const = 0;

    /**
     * Get the number of rows per a entry in the row index.
     * @return the number of rows per an entry in the row index or 0 if there
     * is no row index.
     */
    virtual uint64_t getRowIndexStride() const = 0;

    /**
     * Get the number of stripes in the file.
     * @return the number of stripes
     */
    virtual uint64_t getNumberOfStripes() const = 0;

    /**
     * Get the information about a stripe.
     * @param stripeIndex the stripe 0 to N-1 to get information about
     * @return the information about that stripe
     */
    virtual ORC_UNIQUE_PTR<StripeInformation>
    getStripe(uint64_t stripeIndex) const = 0;

    /**
     * Get the number of stripe statistics in the file.
     * @return the number of stripe statistics
     */
    virtual uint64_t getNumberOfStripeStatistics() const = 0;

    /**
     * Get the statistics about a stripe.
     * @param stripeIndex the stripe 0 to N-1 to get statistics about
     * @return the statistics about that stripe
     */
    virtual ORC_UNIQUE_PTR<Statistics>
    getStripeStatistics(uint64_t stripeIndex) const = 0;

    /**
     * Get the length of the data stripes in the file.
     * @return the number of bytes in stripes
     */
    virtual uint64_t getContentLength() const = 0;

    /**
     * Get the length of the file stripe statistics
     * @return the number of compressed bytes in the file stripe statistics
     */
    virtual uint64_t getStripeStatisticsLength() const = 0;

    /**
     * Get the length of the file footer
     * @return the number of compressed bytes in the file footer
     */
    virtual uint64_t getFileFooterLength() const = 0;

    /**
     * Get the length of the file postscript
     * @return the number of bytes in the file postscript
     */
    virtual uint64_t getFilePostscriptLength() const = 0;

    /**
     * Get the total length of the file.
     * @return the number of bytes in the file
     */
    virtual uint64_t getFileLength() const = 0;

    /**
     * Get the statistics about the columns in the file.
     * @return the information about the column
     */
    virtual ORC_UNIQUE_PTR<Statistics> getStatistics() const = 0;

    /**
     * Get the statistics about a single column in the file.
     * @return the information about the column
     */
    virtual ORC_UNIQUE_PTR<ColumnStatistics>
    getColumnStatistics(uint32_t columnId) const = 0;

    /**
     * check file has correct column statistics
     */
    virtual bool hasCorrectStatistics() const = 0;

    /**
     * Get the serialized file tail.
     * Usefull if another reader of the same file wants to avoid re-reading
     * the file tail. See ReaderOptions.setSerializedFileTail().
     * @return a string of bytes with the file tail
     */
    virtual std::string getSerializedFileTail() const = 0;

    /**
     * Get the type of the rows in the file. The top level is typically a
     * struct.
     * @return the root type
     */
    virtual const Type& getType() const = 0;

    /**
     * Create a RowReader based on this reader with the default options.
     * @return a RowReader to read the rows
     */
    virtual ORC_UNIQUE_PTR<RowReader> createRowReader() const = 0;

    /**
     * Create a RowReader based on this reader.
     * @param options RowReader Options
     * @return a RowReader to read the rows
     */
    virtual ORC_UNIQUE_PTR<RowReader> createRowReader(const RowReaderOptions& options) const = 0;

    /**
     * Get the name of the input stream.
     */
    virtual const std::string& getStreamName() const = 0;

    /**
     * Estimate an upper bound on heap memory allocation by the Reader
     * based on the information in the file footer.
     * The bound is less tight if only few columns are read or compression is
     * used.
    */
    /**
     * @param stripeIx index of the stripe to be read (if not specified,
     *        all stripes are considered).
     * @return upper bound on memory use by all columns
     */
    virtual uint64_t getMemoryUse(int stripeIx=-1) = 0;

    /**
     * @param include Column Field Ids
     * @param stripeIx index of the stripe to be read (if not specified,
     *        all stripes are considered).
     * @return upper bound on memory use by selected columns
     */
    virtual uint64_t getMemoryUseByFieldId(const std::list<uint64_t>& include, int stripeIx=-1) = 0;

    /**
     * @param stripeIx index of the stripe to be read (if not specified,
     *        all stripes are considered).
     * @param names Column Names
     * @return upper bound on memory use by selected columns
     */
    virtual uint64_t getMemoryUseByName(const std::list<std::string>& names, int stripeIx=-1) = 0;

    /**
     * @param stripeIx index of the stripe to be read (if not specified,
     *        all stripes are considered).
     * @param include Column Type Ids
     * @return upper bound on memory use by selected columns
     */
    virtual uint64_t getMemoryUseByTypeId(const std::list<uint64_t>& include, int stripeIx=-1) = 0;

  };

  /**
   * The interface for reading rows in ORC files.
   * This is an an abstract class that will subclassed as necessary.
   */
  class RowReader {
  public:
    virtual ~RowReader();
    /**
     * Get the selected type of the rows in the file. The file's row type
     * is projected down to just the selected columns. Thus, if the file's
     * type is struct<col0:int,col1:double,col2:string> and the selected
     * columns are "col0,col2" the selected type would be
     * struct<col0:int,col2:string>.
     * @return the root type
     */
    virtual const Type& getSelectedType() const = 0;

    /**
     * Get the selected columns of the file.
     */
    virtual const std::vector<bool> getSelectedColumns() const = 0;

    /**
     * Create a row batch for reading the selected columns of this file.
     * @param size the number of rows to read
     * @return a new ColumnVectorBatch to read into
     */
    virtual ORC_UNIQUE_PTR<ColumnVectorBatch> createRowBatch(uint64_t size
                                                             ) const = 0;

    /**
     * Read the next row batch from the current position.
     * Caller must look at numElements in the row batch to determine how
     * many rows were read.
     * @param data the row batch to read into.
     * @return true if a non-zero number of rows were read or false if the
     *   end of the file was reached.
     */
    virtual bool next(ColumnVectorBatch& data) = 0;

    /**
     * Get the row number of the first row in the previously read batch.
     * @return the row number of the previous batch.
     */
    virtual uint64_t getRowNumber() const = 0;

    /**
     * Seek to a given row.
     * @param rowNumber the next row the reader should return
     */
    virtual void seekToRow(uint64_t rowNumber) = 0;

  };
}

#endif
