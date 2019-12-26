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

#include "exprs/mask-functions.h"

#include <gutil/strings/substitute.h>

#include "exprs/anyval-util.h"
#include "util/ubsan.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;

const static int MASKED_UPPERCASE = 'X';
const static int MASKED_LOWERCASE = 'x';
const static int MASKED_DIGIT = 'n';
const static int MASKED_OTHER_CHAR = -1;
const static int MASKED_NUMBER = 1;
const static int MASKED_DAY_COMPONENT_VAL = 1;
const static int MASKED_MONTH_COMPONENT_VAL = 0;
const static int MASKED_YEAR_COMPONENT_VAL = 0;
const static int UNMASKED_VAL = -1;

static uint8_t MaskTransform(uint8_t val, int masked_upper_char, int masked_lower_char,
    int masked_digit_char, int masked_other_char) {
  if ('A' <= val && val <= 'Z') {
    if (masked_upper_char == UNMASKED_VAL) return val;
    return masked_upper_char;
  }
  if ('a' <= val && val <= 'z') {
    if (masked_lower_char == UNMASKED_VAL) return val;
    return masked_lower_char;
  }
  if ('0' <= val && val <= '9') {
    if (masked_digit_char == UNMASKED_VAL) return val;
    return masked_digit_char;
  }
  if (masked_other_char == UNMASKED_VAL) return val;
  return masked_other_char;
}

static StringVal MaskSubStr(FunctionContext* context, const StringVal& val,
    int start, int end, int masked_upper_char, int masked_lower_char,
    int masked_digit_char, int masked_other_char) {
  DCHECK_GE(start, 0);
  DCHECK_LT(start, val.len);
  DCHECK_GT(end, 0);
  DCHECK_LE(end, val.len);
  StringVal result(context, val.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  Ubsan::MemCpy(result.ptr, val.ptr, start);
  if (end < val.len) Ubsan::MemCpy(result.ptr + end, val.ptr + end, val.len - end);
  for (int i = start; i < end; ++i) {
    result.ptr[i] = MaskTransform(val.ptr[i], masked_upper_char, masked_lower_char,
        masked_digit_char, masked_other_char);
  }
  return result;
}

static uint8_t getChar(FunctionContext* context, const StringVal& str) {
  if (str.len != 1) {
    context->SetError(Substitute("Invalid char: $0", AnyValUtil::ToString(str)).c_str());
    return 0;
  }
  return str.ptr[0];
}

//// MaskShowFirstN functions for string value

static StringVal MaskShowFirstNImpl(FunctionContext* context, const StringVal& val,
    int un_mask_char_count, int masked_upper_char, int masked_lower_char,
    int masked_digit_char, int masked_other_char) {
  if (un_mask_char_count < 0) un_mask_char_count = 0;
  if (un_mask_char_count >= val.len) return val;
  return MaskSubStr(context, val, un_mask_char_count, val.len, masked_upper_char,
      masked_lower_char, masked_digit_char, masked_other_char);
}

StringVal MaskFunctions::MaskShowFirstN(FunctionContext* context, const StringVal& val) {
  return MaskShowFirstNImpl(context, val, 4, MASKED_UPPERCASE, MASKED_LOWERCASE,
      MASKED_DIGIT, MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::MaskShowFirstN(FunctionContext* context, const StringVal& val,
    const IntVal& char_count) {
  int un_mask_char_count = char_count.val;
  return MaskShowFirstNImpl(context, val, un_mask_char_count, MASKED_UPPERCASE,
      MASKED_LOWERCASE, MASKED_DIGIT, MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::MaskShowFirstN(FunctionContext* context, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  int un_mask_char_count = char_count.val;
  int masked_upper_char = getChar(context, upper_char);
  int masked_lower_char = getChar(context, lower_char);
  int masked_digit_char = getChar(context, digit_char);
  int masked_other_char = getChar(context, other_char);
  if (masked_upper_char == 0 || masked_lower_char == 0 || masked_digit_char == 0
      || masked_other_char == 0) {
    return StringVal::null();
  }
  return MaskShowFirstNImpl(context, val, un_mask_char_count, masked_upper_char,
      masked_lower_char, masked_digit_char, masked_other_char);
}

StringVal MaskFunctions::MaskShowFirstN(FunctionContext* context, const StringVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return MaskShowFirstNImpl(context, val, char_count.val, upper_char.val, lower_char.val,
      digit_char.val, other_char.val);
}

//// MaskShowFirstN functions for numeric value

static int64_t MaskShowFirstNImpl(int64_t val, int un_mask_char_count, int masked_number) {
  if (un_mask_char_count < 0) un_mask_char_count = 0;
  int64_t unsigned_val = val;
  if (val < 0) unsigned_val = -unsigned_val;
  int num_digits = 0;
  for (int64_t v = unsigned_val; v != 0; v /= 10) {
    num_digits++;
  }
  // number of digits to mask from the end
  int mask_count = num_digits - un_mask_char_count;
  if (mask_count <= 0) return val;
  int64_t result = 0;
  int base = 1;
  for (int i = 0; i < mask_count; ++i) { // loop from end to start
    result += masked_number * base;
    base *= 10;
    unsigned_val /= 10;
  }
  result += unsigned_val * base;
  return val < 0 ? -result : result;
}

BigIntVal MaskFunctions::MaskShowFirstN(FunctionContext* context, const BigIntVal& val) {
  return {MaskShowFirstNImpl(val.val, 4, MASKED_NUMBER)};
}

BigIntVal MaskFunctions::MaskShowFirstN(FunctionContext* context, const BigIntVal& val,
    const IntVal& char_count) {
  return {MaskShowFirstNImpl(val.val, char_count.val, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskShowFirstN(FunctionContext* context, const BigIntVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return {MaskShowFirstNImpl(val.val, char_count.val, number_char.val)};
}
BigIntVal MaskFunctions::MaskShowFirstN(FunctionContext* context, const BigIntVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return {MaskShowFirstNImpl(val.val, char_count.val, number_char.val)};
}

/// MaskShowLastN functions for string value

static StringVal MaskShowLastNImpl(FunctionContext* context, const StringVal& val,
    int un_mask_char_count, int masked_upper_char, int masked_lower_char,
    int masked_digit_char, int masked_other_char) {
  if (un_mask_char_count < 0) un_mask_char_count = 0;
  if (un_mask_char_count >= val.len) return val;
  return MaskSubStr(context, val, 0, val.len - un_mask_char_count, masked_upper_char,
      masked_lower_char, masked_digit_char, masked_other_char);
}

StringVal MaskFunctions::MaskShowLastN(FunctionContext* context, const StringVal& val) {
  return MaskShowLastNImpl(context, val, 4, MASKED_UPPERCASE, MASKED_LOWERCASE,
      MASKED_DIGIT, MASKED_OTHER_CHAR);
}

StringVal MaskFunctions::MaskShowLastN(FunctionContext* context, const StringVal& val,
    const IntVal& char_count) {
  int un_mask_char_count = char_count.val;
  return MaskShowLastNImpl(context, val, un_mask_char_count, MASKED_UPPERCASE,
      MASKED_LOWERCASE, MASKED_DIGIT, MASKED_OTHER_CHAR);
}

StringVal MaskFunctions::MaskShowLastN(FunctionContext* context, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  int un_mask_char_count = char_count.val;
  int masked_upper_char = getChar(context, upper_char);
  int masked_lower_char = getChar(context, lower_char);
  int masked_digit_char = getChar(context, digit_char);
  int masked_other_char = getChar(context, other_char);
  if (masked_upper_char == 0 || masked_lower_char == 0 || masked_digit_char == 0
      || masked_other_char == 0) {
    return StringVal::null();
  }
  return MaskShowLastNImpl(context, val, un_mask_char_count, masked_upper_char,
      masked_lower_char, masked_digit_char, masked_other_char);
}

StringVal MaskFunctions::MaskShowLastN(FunctionContext* context, const StringVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return MaskShowLastNImpl(context, val, char_count.val, upper_char.val, lower_char.val,
      digit_char.val, other_char.val);
}

/// MaskShowLastN functions for numeric value

static int64_t MaskShowLastNImpl(int64_t val, int un_mask_char_count, int masked_number) {
  if (un_mask_char_count < 0) un_mask_char_count = 0;
  int64_t unsigned_val = val;
  if (val < 0) unsigned_val = -unsigned_val;
  int num_digits = 0;
  for (int64_t v = unsigned_val; v != 0; v /= 10) {
    num_digits++;
  }
  int base = 1;
  for (int i = 0; i < un_mask_char_count; ++i) {
    base *= 10;
  }
  int64_t result = unsigned_val % base;
  base *= 10;
  for (int i = un_mask_char_count; i < num_digits; ++i) {
    result += masked_number * base;
    base *= 10;
  }
  return val < 0 ? -result : result;
}

BigIntVal MaskFunctions::MaskShowLastN(FunctionContext* context, const BigIntVal& val) {
  return {MaskShowLastNImpl(val.val, 4, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskShowLastN(FunctionContext* context, const BigIntVal& val,
    const IntVal& char_count) {
  return {MaskShowLastNImpl(val.val, char_count.val, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskShowLastN(FunctionContext* context, const BigIntVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char, const IntVal& number_char) {
  return {MaskShowLastNImpl(val.val, char_count.val, number_char.val)};
}
BigIntVal MaskFunctions::MaskShowLastN(FunctionContext* context, const BigIntVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return {MaskShowLastNImpl(val.val, char_count.val, number_char.val)};
}

/// Mask functions for string value
static StringVal MaskImpl(FunctionContext* context, const StringVal& val,
    int masked_upper_char, int masked_lower_char, int masked_digit_char,
    int masked_other_char) {
  return MaskSubStr(context, val, 0, val.len, masked_upper_char,
      masked_lower_char, masked_digit_char, masked_other_char);
}
StringVal MaskFunctions::Mask(FunctionContext* context, const StringVal& val) {
  return MaskImpl(context, val, MASKED_UPPERCASE, MASKED_LOWERCASE, MASKED_DIGIT,
      MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::Mask(FunctionContext* context, const StringVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char) {
  int masked_upper_char = getChar(context, upper_char);
  int masked_lower_char = getChar(context, lower_char);
  int masked_digit_char = getChar(context, digit_char);
  int masked_other_char = getChar(context, other_char);
  if (masked_upper_char == 0 || masked_lower_char == 0 || masked_digit_char == 0
      || masked_other_char == 0) {
    return StringVal::null();
  }
  return MaskImpl(context, val, masked_upper_char, masked_lower_char, masked_digit_char,
      masked_other_char);
}
StringVal MaskFunctions::Mask(FunctionContext* context, const StringVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return Mask(context, val, upper_char, lower_char, digit_char, other_char);
}
StringVal MaskFunctions::Mask(FunctionContext* context, const StringVal& val,
    const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
    const IntVal& other_char, const IntVal& number_char, const IntVal& day_value,
    const IntVal& month_value, const IntVal& year_value) {
  return MaskImpl(context, val, upper_char.val, lower_char.val, digit_char.val,
      other_char.val);
}

/// Mask functions for Date value
DateVal MaskImpl(const DateVal& val, int day_value, int month_value, int year_value) {
  int year, month, day;
  DateValue dv = DateValue::FromDateVal(val);
  if (!dv.ToYearMonthDay(&year, &month, &day)) return DateVal::null();
  if (year_value != UNMASKED_VAL) year = day_value;
  if (month_value != UNMASKED_VAL) month = month_value + 1;
  if (day_value != UNMASKED_VAL) day = year_value;
  return DateValue(year, month, day).ToDateVal();
}
DateVal MaskFunctions::Mask(FunctionContext* context, const DateVal& val) {
  return MaskImpl(val, MASKED_DAY_COMPONENT_VAL, MASKED_MONTH_COMPONENT_VAL,
      MASKED_YEAR_COMPONENT_VAL);
}
DateVal MaskFunctions::Mask(FunctionContext* context, const DateVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char, const IntVal& day_value) {
  return MaskImpl(val, day_value.val, MASKED_MONTH_COMPONENT_VAL,
      MASKED_YEAR_COMPONENT_VAL);
}
DateVal MaskFunctions::Mask(FunctionContext* context, const DateVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char, const IntVal& day_value, const IntVal& month_value) {
  return MaskImpl(val, day_value.val, month_value.val, MASKED_YEAR_COMPONENT_VAL);
}
DateVal MaskFunctions::Mask(FunctionContext* context, const DateVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return MaskImpl(val, year_value.val, month_value.val, day_value.val);
}

/// Mask functions for numeric value
BigIntVal MaskFunctions::Mask(FunctionContext* context, const BigIntVal& val) {
  return {MaskShowFirstNImpl(val.val, 0, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::Mask(FunctionContext* context, const BigIntVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return {MaskShowFirstNImpl(val.val, 0, number_char.val)};
}
BigIntVal MaskFunctions::Mask(FunctionContext* context, const BigIntVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return {MaskShowFirstNImpl(val.val, 0, number_char.val)};
}
BigIntVal MaskFunctions::Mask(FunctionContext* context, const BigIntVal& val,
    const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
    const IntVal& other_char, const IntVal& number_char, const IntVal& day_value,
    const IntVal& month_value, const IntVal& year_value) {
  return {MaskShowFirstNImpl(val.val, 0, number_char.val)};
}