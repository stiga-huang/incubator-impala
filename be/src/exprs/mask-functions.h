// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#pragma once

#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::AnyVal;
using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::DateVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;

class MaskFunctions {
 public:
  /// Implementations of mask_show_first_n()
  /// Overloads for masking a string value
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val);
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count);
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);
  /// Overloads for masking a numeric value
  static BigIntVal MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val);
  static BigIntVal MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count);
  static BigIntVal MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static BigIntVal MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);

  /// Implementations of mask_show_last_n()
  /// Overloads for masking a string value
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val);
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count);
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);
  /// Overloads for masking a numeric value
  static BigIntVal MaskShowLastN(FunctionContext* ctx, const BigIntVal& val);
  static BigIntVal MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count);
  static BigIntVal MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static BigIntVal MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);

  /// Implementations of mask_first_n()
  /// Overloads for masking a string value
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val);
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count);
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);
  /// Overloads for masking a numeric value
  static BigIntVal MaskFirstN(FunctionContext* ctx, const BigIntVal& val);
  static BigIntVal MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count);
  static BigIntVal MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static BigIntVal MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);

  /// Implementations of mask_first_n()
  /// Overloads for masking a string value
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val);
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count);
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);
  /// Overloads for masking a numeric value
  static BigIntVal MaskLastN(FunctionContext* ctx, const BigIntVal& val);
  static BigIntVal MaskLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count);
  static BigIntVal MaskLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static BigIntVal MaskLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);

  /// Implementations of mask()
  /// Overloads for masking a string value
  static StringVal Mask(FunctionContext* ctx, const StringVal& val);
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char);
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
      const IntVal& other_char, const IntVal& number_char, const IntVal& day_value,
      const IntVal& month_value, const IntVal& year_value);
  /// Overloads for masking a date value
  static DateVal Mask(FunctionContext* ctx, const DateVal& val);
  static DateVal Mask(FunctionContext* ctx, const DateVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char, const IntVal& day_value);
  static DateVal Mask(FunctionContext* ctx, const DateVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char, const IntVal& day_value, const IntVal& month_value);
  static DateVal Mask(FunctionContext* ctx, const DateVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  /// Overloads for masking a numeric value
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val);
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
      const IntVal& other_char, const IntVal& number_char, const IntVal& day_value,
      const IntVal& month_value, const IntVal& year_value);
};

} // namespace impala
