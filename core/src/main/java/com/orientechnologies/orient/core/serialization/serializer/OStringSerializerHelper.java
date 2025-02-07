/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.serialization.serializer;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.types.OBinary;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerAnyStreamable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class OStringSerializerHelper {
  public static final char RECORD_SEPARATOR = ',';

  public static final String CLASS_SEPARATOR = "@";
  public static final char LINK = ORID.PREFIX;
  public static final char EMBEDDED_BEGIN = '(';
  public static final char EMBEDDED_END = ')';
  public static final char LIST_BEGIN = '[';
  public static final char LIST_END = ']';
  public static final char SET_BEGIN = '<';
  public static final String LINKSET_PREFIX = "" + SET_BEGIN + LINK + CLASS_SEPARATOR;
  public static final char SET_END = '>';
  public static final char MAP_BEGIN = '{';
  public static final char MAP_END = '}';
  public static final char BAG_BEGIN = '%';
  public static final char BAG_END = ';';
  public static final char BINARY_BEGINEND = '_';
  public static final char CUSTOM_TYPE = '^';
  public static final char ENTRY_SEPARATOR = ':';
  public static final char PARAMETER_NAMED = ':';
  public static final char PARAMETER_POSITIONAL = '?';
  public static final char[] PARAMETER_SEPARATOR = new char[] {','};
  public static final char[] PARAMETER_EXT_SEPARATOR = new char[] {' ', '.'};
  public static final char[] DEFAULT_IGNORE_CHARS = new char[] {'\n', '\r', ' '};
  public static final char[] DEFAULT_FIELD_SEPARATOR = new char[] {',', ' '};
  public static final char COLLECTION_SEPARATOR = ',';
  public static final String SKIPPED_VALUE = "[SKIPPED VALUE]";

  public static Object fieldTypeFromStream(
      final ODocument iDocument, OType iType, final Object iValue) {
    if (iValue == null) return null;

    if (iType == null) iType = OType.EMBEDDED;

    switch (iType) {
      case STRING:
        if (iValue instanceof String) {
          final String s = (String) iValue;
          return decode(OIOUtils.getStringContent(s));
        }
        return iValue.toString();

      case INTEGER:
        {
          if (iValue instanceof Integer) {
            return iValue;
          }
          final String valueString = OIOUtils.getStringContent(iValue);
          if (valueString.isEmpty()) {
            return null;
          }
          return Integer.parseInt(valueString);
        }

      case BOOLEAN:
        {
          if (iValue instanceof Boolean) {
            return iValue;
          }
          final String valueString = OIOUtils.getStringContent(iValue);
          if (valueString.isEmpty()) {
            return null;
          }
          return Boolean.parseBoolean(valueString);
        }

      case DECIMAL:
        {
          if (iValue instanceof BigDecimal) {
            return iValue;
          }
          final String valueString = OIOUtils.getStringContent(iValue);
          if (valueString.isEmpty()) {
            return null;
          }
          return new BigDecimal(valueString);
        }

      case FLOAT:
        {
          if (iValue instanceof Float) {
            return iValue;
          }
          final String valueString = OIOUtils.getStringContent(iValue);
          if (valueString.isEmpty()) {
            return null;
          }
          return Float.parseFloat(valueString);
        }

      case LONG:
        {
          if (iValue instanceof Long) {
            return iValue;
          }
          final String valueString = OIOUtils.getStringContent(iValue);
          if (valueString.isEmpty()) {
            return null;
          }
          return Long.parseLong(valueString);
        }

      case DOUBLE:
        {
          if (iValue instanceof Double) {
            return iValue;
          }
          final String valueString = OIOUtils.getStringContent(iValue);
          if (valueString.isEmpty()) {
            return null;
          }
          return Double.parseDouble(valueString);
        }

      case SHORT:
        {
          if (iValue instanceof Short) {
            return iValue;
          }
          final String valueString = OIOUtils.getStringContent(iValue);
          if (valueString.isEmpty()) {
            return null;
          }
          return Short.parseShort(valueString);
        }

      case BYTE:
        {
          if (iValue instanceof Byte) {
            return iValue;
          }
          final String valueString = OIOUtils.getStringContent(iValue);
          if (valueString.isEmpty()) {
            return null;
          }
          return Byte.parseByte(valueString);
        }

      case BINARY:
        return getBinaryContent(iValue);

      case DATE:
      case DATETIME:
        if (iValue instanceof Date) {
          return iValue;
        }
        return new Date(Long.parseLong(OIOUtils.getStringContent(iValue)));

      case LINK:
        if (iValue instanceof ORID) return iValue.toString();
        else if (iValue instanceof String) return new ORecordId((String) iValue);
        else return ((ORecord) iValue).getIdentity().toString();

      case EMBEDDED:
        // EMBEDDED
        return OStringSerializerAnyStreamable.INSTANCE.fromStream((String) iValue);

      case EMBEDDEDMAP:
        // RECORD
        final String value = (String) iValue;
        return ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedMapFromStream(
            iDocument, null, value, null);

      case ANY:
        if (iValue instanceof String) {
          final String s = (String) iValue;
          return decode(OIOUtils.getStringContent(s));
        }
        return iValue;
    }

    throw new IllegalArgumentException(
        "Type " + iType + " does not support converting value: " + iValue);
  }

  public static String smartTrim(
      String source, final boolean removeLeadingSpaces, final boolean removeTailingSpaces) {
    int startIndex = 0;
    int length = source.length();

    while (startIndex < length && source.charAt(startIndex) == ' ') {
      startIndex++;
    }

    if (!removeLeadingSpaces && startIndex > 0) startIndex--;

    while (length > startIndex && source.charAt(length - 1) == ' ') {
      length--;
    }

    if (!removeTailingSpaces && length < source.length()) length++;

    return source.substring(startIndex, length);
  }

  public static List<String> smartSplit(
      final String iSource,
      final char iRecordSeparator,
      boolean iPreserveQuotes,
      final char... iJumpChars) {
    return smartSplit(
        iSource,
        new char[] {iRecordSeparator},
        0,
        -1,
        true,
        true,
        false,
        false,
        true,
        iPreserveQuotes,
        iJumpChars);
  }

  public static List<String> smartSplit(
      final String iSource, final char iRecordSeparator, final char... iJumpChars) {
    return smartSplit(
        iSource, new char[] {iRecordSeparator}, 0, -1, true, true, false, false, iJumpChars);
  }

  public static List<String> smartSplit(
      final String iSource,
      final char iRecordSeparator,
      final boolean iConsiderSets,
      boolean considerBags,
      final char... iJumpChars) {
    return smartSplit(
        iSource,
        new char[] {iRecordSeparator},
        0,
        -1,
        false,
        true,
        iConsiderSets,
        considerBags,
        iJumpChars);
  }

  public static List<String> smartSplit(
      final String iSource,
      final char[] iRecordSeparator,
      int beginIndex,
      final int endIndex,
      final boolean iStringSeparatorExtended,
      final boolean iConsiderBraces,
      final boolean iConsiderSets,
      final boolean iConsiderBags,
      final char... iJumpChars) {
    return smartSplit(
        iSource,
        iRecordSeparator,
        beginIndex,
        endIndex,
        iStringSeparatorExtended,
        iConsiderBraces,
        iConsiderSets,
        iConsiderBags,
        true,
        iJumpChars);
  }

  public static List<String> smartSplit(
      final String iSource,
      final char[] iRecordSeparator,
      int beginIndex,
      final int endIndex,
      final boolean iStringSeparatorExtended,
      final boolean iConsiderBraces,
      final boolean iConsiderSets,
      final boolean iConsiderBags,
      final int maxRidbagSizeBeforeSkip,
      Set<Integer> skippedPartsIndexes,
      char... iJumpChars) {
    return smartSplit(
        iSource,
        iRecordSeparator,
        beginIndex,
        endIndex,
        iStringSeparatorExtended,
        iConsiderBraces,
        iConsiderSets,
        iConsiderBags,
        true,
        maxRidbagSizeBeforeSkip,
        skippedPartsIndexes,
        iJumpChars);
  }

  public static List<String> smartSplit(
      final String iSource,
      final char[] iRecordSeparator,
      int beginIndex,
      final int endIndex,
      final boolean iStringSeparatorExtended,
      final boolean iConsiderBraces,
      final boolean iConsiderSets,
      final boolean iConsiderBags,
      boolean iUnicode,
      final char... iJumpChars) {
    return smartSplit(
        iSource,
        iRecordSeparator,
        beginIndex,
        endIndex,
        iStringSeparatorExtended,
        iConsiderBraces,
        iConsiderSets,
        iConsiderBags,
        iUnicode,
        false,
        iJumpChars);
  }

  public static List<String> smartSplit(
      final String iSource,
      final char[] iRecordSeparator,
      int beginIndex,
      final int endIndex,
      final boolean iStringSeparatorExtended,
      final boolean iConsiderBraces,
      final boolean iConsiderSets,
      final boolean iConsiderBags,
      boolean iUnicode,
      final int maxRidbagSizeBeforeSkip,
      final Set<Integer> skippedPartsIndexes,
      final char... iJumpChars) {
    return smartSplit(
        iSource,
        iRecordSeparator,
        beginIndex,
        endIndex,
        iStringSeparatorExtended,
        iConsiderBraces,
        iConsiderSets,
        iConsiderBags,
        iUnicode,
        false,
        maxRidbagSizeBeforeSkip,
        skippedPartsIndexes,
        iJumpChars);
  }

  public static List<String> smartSplit(
      final String iSource,
      final char[] iRecordSeparator,
      int beginIndex,
      final int endIndex,
      final boolean iStringSeparatorExtended,
      final boolean iConsiderBraces,
      final boolean iConsiderSets,
      final boolean iConsiderBags,
      boolean iUnicode,
      boolean iPreserveQuotes,
      final char... iJumpChars) {

    return smartSplit(
        iSource,
        iRecordSeparator,
        beginIndex,
        endIndex,
        iStringSeparatorExtended,
        iConsiderBraces,
        iConsiderSets,
        iConsiderBags,
        iUnicode,
        iPreserveQuotes,
        -1,
        new HashSet<>(),
        iJumpChars);
  }

  public static List<String> smartSplit(
      final String iSource,
      final char[] iRecordSeparator,
      int beginIndex,
      final int endIndex,
      final boolean iStringSeparatorExtended,
      final boolean iConsiderBraces,
      final boolean iConsiderSets,
      final boolean iConsiderBags,
      boolean iUnicode,
      boolean iPreserveQuotes,
      final int maxRidbagSizeBeforeSkip,
      Set<Integer> skippedPartsIndexes,
      final char... iJumpChars) {

    final StringBuilder buffer = new StringBuilder(128);
    final ArrayList<String> parts = new ArrayList<String>();

    int previousBegin1 = beginIndex;
    int previousBegin2 = beginIndex;
    if (iSource != null && !iSource.isEmpty()) {
      while ((beginIndex =
              parse(
                  iSource,
                  buffer,
                  beginIndex,
                  endIndex,
                  iRecordSeparator,
                  iStringSeparatorExtended,
                  iConsiderBraces,
                  iConsiderSets,
                  -1,
                  iConsiderBags,
                  iUnicode,
                  iPreserveQuotes,
                  (parts.size() % 2 == 1
                          && (parts.get(parts.size() - 1).startsWith("\"out_")
                              || parts.get(parts.size() - 1).startsWith("\"in_")))
                      ? maxRidbagSizeBeforeSkip
                      : -1,
                  iJumpChars))
          > -1) {
        parts.add(buffer.toString());
        if (buffer.toString().equals(SKIPPED_VALUE)) {
          skippedPartsIndexes.add(previousBegin2);
        }
        buffer.setLength(0);
        previousBegin2 = previousBegin1;
        previousBegin1 = beginIndex;
      }

      if (buffer.length() > 0
          || isCharPresent(iSource.charAt(iSource.length() - 1), iRecordSeparator))
        parts.add(buffer.toString());
    }

    return parts;
  }

  public static List<String> smartSplit(
      final String iSource,
      final char[] iRecordSeparator,
      final boolean[] iRecordSeparatorIncludeAsPrefix,
      final boolean[] iRecordSeparatorIncludeAsPostfix,
      int beginIndex,
      final int endIndex,
      final boolean iStringSeparatorExtended,
      boolean iConsiderBraces,
      boolean iConsiderSets,
      boolean considerBags,
      final char... iJumpChars) {
    final StringBuilder buffer = new StringBuilder(128);
    final ArrayList<String> parts = new ArrayList<String>();

    int startSeparatorAt = -1;
    if (iSource != null && !iSource.isEmpty()) {

      while ((beginIndex =
              parse(
                  iSource,
                  buffer,
                  beginIndex,
                  endIndex,
                  iRecordSeparator,
                  iStringSeparatorExtended,
                  iConsiderBraces,
                  iConsiderSets,
                  startSeparatorAt,
                  considerBags,
                  true,
                  iJumpChars))
          > -1) {

        if (beginIndex > -1) {
          final char lastSeparator = iSource.charAt(beginIndex - 1);
          for (int i = 0; i < iRecordSeparator.length; ++i)
            if (iRecordSeparator[i] == lastSeparator) {
              if (iRecordSeparatorIncludeAsPrefix[i]) {
                buffer.append(lastSeparator);
              }
              break;
            }
        }

        if (buffer.length() > 0) {
          parts.add(buffer.toString());
          buffer.setLength(0);
        }

        startSeparatorAt = beginIndex;

        if (beginIndex > -1) {
          final char lastSeparator = iSource.charAt(beginIndex - 1);
          for (int i = 0; i < iRecordSeparator.length; ++i)
            if (iRecordSeparator[i] == lastSeparator) {
              if (iRecordSeparatorIncludeAsPostfix[i]) {
                beginIndex--;
                startSeparatorAt = beginIndex + 1;
              }
              break;
            }
        }
      }

      if (buffer.length() > 0) parts.add(buffer.toString());
    }

    return parts;
  }

  public static int parse(
      final String iSource,
      final StringBuilder iBuffer,
      final int beginIndex,
      final int endIndex,
      final char[] iSeparator,
      final boolean iStringSeparatorExtended,
      final boolean iConsiderBraces,
      final boolean iConsiderSets,
      final int iMinPosSeparatorAreValid,
      boolean considerBags,
      final char... iJumpChars) {
    return parse(
        iSource,
        iBuffer,
        beginIndex,
        endIndex,
        iSeparator,
        iStringSeparatorExtended,
        iConsiderBraces,
        iConsiderSets,
        iMinPosSeparatorAreValid,
        considerBags,
        true,
        false,
        -1,
        iJumpChars);
  }

  public static int parse(
      final String iSource,
      final StringBuilder iBuffer,
      final int beginIndex,
      final int endIndex,
      final char[] iSeparator,
      final boolean iStringSeparatorExtended,
      final boolean iConsiderBraces,
      final boolean iConsiderSets,
      final int iMinPosSeparatorAreValid,
      boolean considerBags,
      boolean iPreserveQuotes,
      final char... iJumpChars) {
    return parse(
        iSource,
        iBuffer,
        beginIndex,
        endIndex,
        iSeparator,
        iStringSeparatorExtended,
        iConsiderBraces,
        iConsiderSets,
        iMinPosSeparatorAreValid,
        considerBags,
        true,
        iPreserveQuotes,
        -1,
        iJumpChars);
  }

  public static int parse(
      final String iSource,
      final StringBuilder iBuffer,
      final int beginIndex,
      final int endIndex,
      final char[] iSeparator,
      final boolean iStringSeparatorExtended,
      final boolean iConsiderBraces,
      final boolean iConsiderSets,
      final int iMinPosSeparatorAreValid,
      boolean considerBags,
      final boolean iUnicode,
      boolean iPreserveQuotes,
      final int iMaxValueSizeBeforeSkip,
      final char... iJumpChars) {
    if (beginIndex < 0) return beginIndex;

    char stringBeginChar = ' ';
    boolean encodeMode = false;
    int insideParenthesis = 0;
    int insideList = 0;
    int insideSet = 0;
    int insideMap = 0;
    int insideLinkPart = 0;
    int insideBag = 0;

    boolean tooBigValue = false;

    final int max = endIndex > -1 ? endIndex + 1 : iSource.length();

    //    iBuffer.ensureCapacity(max);

    // JUMP FIRST CHARS
    int i = beginIndex;
    for (; i < max; ++i) {
      final char c = iSource.charAt(i);
      if (!isCharPresent(c, iJumpChars)) break;
    }

    for (; i < max; ++i) {
      final char c = iSource.charAt(i);

      if (stringBeginChar == ' ') {
        // OUTSIDE A STRING

        if (iConsiderBraces) {
          if (c == LIST_BEGIN) {
            if (i < iMinPosSeparatorAreValid
                || insideParenthesis > 0
                || insideList > 0
                || !isCharPresent(c, iSeparator)) insideList++;
          } else if (c == LIST_END) {
            if (i < iMinPosSeparatorAreValid
                || insideParenthesis > 0
                || insideList > 0
                || !isCharPresent(c, iSeparator)) {
              if (insideList == 0)
                throw new OSerializationException(
                    "Found invalid "
                        + LIST_END
                        + " character at position "
                        + i
                        + " of text "
                        + new String(iSource)
                        + ". Ensure it is opened and closed correctly.");
              insideList--;
            }
          } else if (c == EMBEDDED_BEGIN) {
            insideParenthesis++;
          } else if (c == EMBEDDED_END) {
            // if (!isCharPresent(c, iRecordSeparator)) {
            if (insideParenthesis == 0)
              throw new OSerializationException(
                  "Found invalid "
                      + EMBEDDED_END
                      + " character at position "
                      + i
                      + " of text "
                      + new String(iSource)
                      + ". Ensure it is opened and closed correctly.");
            // }
            insideParenthesis--;

          } else if (c == MAP_BEGIN) {
            insideMap++;
          } else if (c == MAP_END) {
            if (i < iMinPosSeparatorAreValid || !isCharPresent(c, iSeparator)) {
              if (insideMap == 0)
                throw new OSerializationException(
                    "Found invalid "
                        + MAP_END
                        + " character at position "
                        + i
                        + " of text "
                        + new String(iSource)
                        + ". Ensure it is opened and closed correctly.");
              insideMap--;
            }
          } else if (c == LINK)
            // FIRST PART OF LINK
            insideLinkPart = 1;
          else if (insideLinkPart == 1 && c == ORID.SEPARATOR)
            // SECOND PART OF LINK
            insideLinkPart = 2;
          else {
            if (iConsiderSets)
              if (c == SET_BEGIN) insideSet++;
              else if (c == SET_END) {
                if (i < iMinPosSeparatorAreValid || !isCharPresent(c, iSeparator)) {
                  if (insideSet == 0)
                    throw new OSerializationException(
                        "Found invalid "
                            + SET_END
                            + " character at position "
                            + i
                            + " of text "
                            + new String(iSource)
                            + ". Ensure it is opened and closed correctly.");
                  insideSet--;
                }
              }
            if (considerBags) {
              if (c == BAG_BEGIN) insideBag++;
              else if (c == BAG_END)
                if (!isCharPresent(c, iSeparator)) {
                  if (insideBag == 0)
                    throw new OSerializationException(
                        "Found invalid "
                            + BAG_BEGIN
                            + " character. Ensure it is opened and closed correctly.");
                  insideBag--;
                }
            }
          }
        }

        if (insideLinkPart > 0
            && c != '-'
            && !Character.isDigit(c)
            && c != ORID.SEPARATOR
            && c != LINK) insideLinkPart = 0;

        if ((c == '"' || c == '`' || iStringSeparatorExtended && c == '\'') && !encodeMode) {
          // START STRING
          stringBeginChar = c;
        }

        if (insideParenthesis == 0
            && insideList == 0
            && insideSet == 0
            && insideMap == 0
            && insideLinkPart == 0
            && insideBag == 0) {
          // OUTSIDE A PARAMS/COLLECTION/MAP
          if (i >= iMinPosSeparatorAreValid && isCharPresent(c, iSeparator)) {
            // SEPARATOR (OUTSIDE A STRING): PUSH
            return i + 1;
          }
        }

        if (iJumpChars.length > 0)
          if (i >= iMinPosSeparatorAreValid && isCharPresent(c, iJumpChars)) continue;
      } else {
        // INSIDE A STRING
        if ((c == '"' || c == '`' || iStringSeparatorExtended && c == '\'') && !encodeMode) {
          // CLOSE THE STRING ?
          if (stringBeginChar == c) {
            // SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
            stringBeginChar = ' ';
          }
        }
      }

      if (c == '\\' && !encodeMode && !iPreserveQuotes) {
        // ESCAPE CHARS
        final char nextChar = iSource.charAt(i + 1);
        if (nextChar == 'u' && iUnicode) {
          i =
              OStringParser.readUnicode(
                  iSource, i + 2, tooBigValue ? new StringBuilder() : iBuffer);
          continue;
        } else if (nextChar == 'n') {
          if (!tooBigValue) {
            iBuffer.append("\n");
          }
          i++;
          continue;
        } else if (nextChar == 'r') {
          if (!tooBigValue) {
            iBuffer.append("\r");
          }
          i++;
          continue;
        } else if (nextChar == 't') {
          if (!tooBigValue) {
            iBuffer.append("\t");
          }
          i++;
          continue;
        } else if (nextChar == 'f') {
          if (!tooBigValue) {
            iBuffer.append("\f");
          }
          i++;
          continue;
        } else encodeMode = true;
      } else encodeMode = false;

      if (c != '\\' && encodeMode) {
        encodeMode = false;
      }
      if (iMaxValueSizeBeforeSkip > 0 && iBuffer.length() > iMaxValueSizeBeforeSkip) {
        tooBigValue = true;
        iBuffer.setLength(0);
        iBuffer.append(SKIPPED_VALUE);
      }
      if (!tooBigValue) {
        iBuffer.append(c);
      }
    }
    return -1;
  }

  public static boolean isCharPresent(final char iCharacter, final char[] iCharacters) {
    final int len = iCharacters.length;
    for (int i = 0; i < len; ++i) {
      if (iCharacter == iCharacters[i]) {
        return true;
      }
    }

    return false;
  }

  public static List<String> split(
      final String iSource, final char iRecordSeparator, final char... iJumpCharacters) {
    return split(iSource, 0, iSource.length(), iRecordSeparator, iJumpCharacters);
  }

  public static Collection<String> split(
      final Collection<String> iParts,
      final String iSource,
      final char iRecordSeparator,
      final char... iJumpCharacters) {
    return split(iParts, iSource, 0, iSource.length(), iRecordSeparator, iJumpCharacters);
  }

  public static List<String> split(
      final String iSource,
      final int iStartPosition,
      final int iEndPosition,
      final char iRecordSeparator,
      final char... iJumpCharacters) {
    return (List<String>)
        split(
            new ArrayList<String>(),
            iSource,
            iStartPosition,
            iSource.length(),
            iRecordSeparator,
            iJumpCharacters);
  }

  public static Collection<String> split(
      final Collection<String> iParts,
      final String iSource,
      final int iStartPosition,
      final int iEndPosition,
      final char iRecordSeparator,
      final char... iJumpCharacters) {
    return split(
        iParts,
        iSource,
        iStartPosition,
        iEndPosition,
        String.valueOf(iRecordSeparator),
        iJumpCharacters);
  }

  public static Collection<String> split(
      final Collection<String> iParts,
      final String iSource,
      final int iStartPosition,
      int iEndPosition,
      final String iRecordSeparators,
      final char... iJumpCharacters) {
    if (iEndPosition == -1) iEndPosition = iSource.length();

    final StringBuilder buffer = new StringBuilder(128);

    for (int i = iStartPosition; i < iEndPosition; ++i) {
      char c = iSource.charAt(i);

      if (iRecordSeparators.indexOf(c) > -1) {
        iParts.add(buffer.toString());
        buffer.setLength(0);
      } else {
        if (iJumpCharacters.length > 0 && buffer.length() == 0) {
          // CHECK IF IT'S A CHAR TO JUMP
          if (!isCharPresent(c, iJumpCharacters)) {
            buffer.append(c);
          }
        } else buffer.append(c);
      }
    }

    if (iJumpCharacters.length > 0 && buffer.length() > 0) {
      // CHECK THE END OF LAST ITEM IF NEED TO CUT THE CHARS TO JUMP
      char b;
      int newSize = 0;
      boolean found;
      for (int i = buffer.length() - 1; i >= 0; --i) {
        b = buffer.charAt(i);
        found = false;
        for (char j : iJumpCharacters) {
          if (j == b) {
            found = true;
            ++newSize;
            break;
          }
        }
        if (!found) break;
      }
      if (newSize > 0) buffer.setLength(buffer.length() - newSize);
    }

    iParts.add(buffer.toString());

    return iParts;
  }

  public static int[] splitIntArray(final String iInput) {
    final List<String> items = split(iInput, RECORD_SEPARATOR);
    final int[] values = new int[items.size()];
    for (int i = 0; i < items.size(); ++i) {
      values[i] = Integer.parseInt(items.get(i).trim());
    }
    return values;
  }

  public static boolean contains(final String iText, final char iSeparator) {
    if (iText == null) return false;

    return iText.indexOf(iSeparator) > -1;
  }

  public static int getCollection(
      final String iText,
      final int iStartPosition,
      final Collection<String> iCollection,
      final char iCollectionBegin,
      final char iCollectionEnd,
      final char iCollectionSeparator) {
    int openPos = iText.indexOf(iCollectionBegin, iStartPosition);
    if (openPos == -1) return -1;

    final StringBuilder buffer = new StringBuilder(128);

    boolean escape = false;
    char insideQuote = ' ';
    int currentPos;
    int deep;
    int maxPos = iText.length() - 1;
    for (currentPos = openPos + 1, deep = 1; deep > 0; currentPos++) {
      if (currentPos > maxPos) return -1;

      char c = iText.charAt(currentPos);

      if (buffer.length() == 0 && c == ' ') continue;

      if (c == iCollectionBegin) {
        // BEGIN
        buffer.append(c);
        deep++;
      } else if (c == iCollectionEnd) {
        // END
        if (deep > 1) buffer.append(c);
        deep--;
      } else if (c == iCollectionSeparator) {
        // SEPARATOR
        if (deep > 1 || insideQuote != ' ') {
          buffer.append(c);
        } else {
          iCollection.add(buffer.toString().trim());
          buffer.setLength(0);
        }
      } else if (!escape
          && ((insideQuote == ' ' && (c == '"' || c == '\'')) || (insideQuote == c))) {
        insideQuote = insideQuote == ' ' ? c : ' ';
        buffer.append(c);
      } else {
        // COLLECT
        if (!escape && c == '\\' && (currentPos + 1 <= maxPos)) {
          // ESCAPE CHARS
          final char nextChar = iText.charAt(currentPos + 1);

          if (nextChar == 'u') {
            currentPos = OStringParser.readUnicode(iText, currentPos + 2, buffer);
          } else if (nextChar == 'n') {
            buffer.append("\n");
            currentPos++;
          } else if (nextChar == 'r') {
            buffer.append("\r");
            currentPos++;
          } else if (nextChar == 't') {
            buffer.append("\t");
            currentPos++;
          } else if (nextChar == 'f') {
            buffer.append("\f");
            currentPos++;
          } else escape = true;

          continue;
        }
        escape = false;
        buffer.append(c);
      }
    }

    if (buffer.length() > 0) iCollection.add(buffer.toString().trim());

    return --currentPos;
  }

  public static Map<String, String> getMap(final String iText) {
    int openPos = iText.indexOf(MAP_BEGIN);
    if (openPos == -1) return Collections.emptyMap();

    int closePos = iText.indexOf(MAP_END, openPos + 1);
    if (closePos == -1) return Collections.emptyMap();

    final List<String> entries =
        smartSplit(iText.substring(openPos + 1, closePos), COLLECTION_SEPARATOR);
    if (entries.size() == 0) return Collections.emptyMap();

    Map<String, String> map = new HashMap<String, String>();

    List<String> entry;
    for (String item : entries) {
      if (item != null && !item.isEmpty()) {
        entry = OStringSerializerHelper.split(item, OStringSerializerHelper.ENTRY_SEPARATOR);

        final String key = entry.get(0).trim();
        final String value = entry.get(1).trim();

        map.put((String) fieldTypeFromStream(null, OType.STRING, key), value);
      }
    }

    return map;
  }

  /**
   * Transforms, only if needed, the source string escaping the characters \ and ".
   *
   * @param iText Input String
   * @return Modified string if needed, otherwise the same input object
   * @see OStringSerializerHelper#decode(String)
   */
  public static String encode(final String iText) {
    int pos = -1;

    final int newSize = iText.length();
    for (int i = 0; i < newSize; ++i) {
      final char c = iText.charAt(i);

      if (c == '"' || c == '\\') {
        pos = i;
        break;
      }
    }

    if (pos > -1) {
      // CHANGE THE INPUT STRING
      final StringBuilder iOutput = new StringBuilder((int) ((float) newSize * 1.5f));

      char c;
      for (int i = 0; i < newSize; ++i) {
        c = iText.charAt(i);

        if (c == '"' || c == '\\') iOutput.append('\\');

        iOutput.append(c);
      }
      return iOutput.toString();
    }

    return iText;
  }

  /**
   * Transforms, only if needed, the source string un-escaping the characters \ and ".
   *
   * @param iText Input String
   * @return Modified string if needed, otherwise the same input object
   * @see OStringSerializerHelper#encode(String)
   */
  public static String decode(final String iText) {
    int pos = -1;

    final int textSize = iText.length();
    for (int i = 0; i < textSize; ++i)
      if (iText.charAt(i) == '"' || iText.charAt(i) == '\\') {
        pos = i;
        break;
      }

    if (pos == -1)
      // NOT FOUND, RETURN THE SAME STRING (AVOID COPIES)
      return iText;

    // CHANGE THE INPUT STRING
    final StringBuilder buffer = new StringBuilder(textSize);
    buffer.append(iText.substring(0, pos));

    boolean escaped = false;
    for (int i = pos; i < textSize; ++i) {
      final char c = iText.charAt(i);

      if (escaped) {
        escaped = false;
        if (c == 'n') {
          buffer.append('\n');
        } else if (c == 't') {
          buffer.append('\t');
        } else if (c == 'r') {
          buffer.append('\r');
        } else if (c == '\\') {
          buffer.append('\\');
        } else if (c == 'f') {
          buffer.append('\f');
        } else if (c == 'b') {
          buffer.append('\b');
        } else {
          buffer.append(c);
        }
        continue;
      } else if (c == '\\') {
        escaped = true;
        continue;
      }

      buffer.append(c);
    }

    return buffer.toString();
  }

  public static OClass getRecordClassName(final String iValue, OClass iLinkedClass) {
    // EXTRACT THE CLASS NAME
    final int classSeparatorPos =
        OStringParser.indexOfOutsideStrings(
            iValue, OStringSerializerHelper.CLASS_SEPARATOR.charAt(0), 0, -1);
    if (classSeparatorPos > -1) {
      final String className = iValue.substring(0, classSeparatorPos);
      final ODatabaseDocument database = ODatabaseRecordThreadLocal.instance().get();
      if (className != null && database != null)
        iLinkedClass =
            ((OMetadataInternal) database.getMetadata())
                .getImmutableSchemaSnapshot()
                .getClass(className);
    }
    return iLinkedClass;
  }

  /**
   * Returns the binary representation of a content. If it's a String a Base64 decoding is applied.
   */
  public static byte[] getBinaryContent(final Object iValue) {
    if (iValue == null) return null;
    else if (iValue instanceof OBinary) return ((OBinary) iValue).toByteArray();
    else if (iValue instanceof byte[]) return (byte[]) iValue;
    else if (iValue instanceof String) {
      String s = (String) iValue;
      if (s.length() > 1
              && (s.charAt(0) == BINARY_BEGINEND && s.charAt(s.length() - 1) == BINARY_BEGINEND)
          || (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\''))
        // @COMPATIBILITY 1.0rc7-SNAPSHOT ' TO SUPPORT OLD DATABASES
        s = s.substring(1, s.length() - 1);
      // IN CASE OF JSON BINARY IMPORT THIS EXEPTION IS WRONG
      // else
      // throw new IllegalArgumentException("Not binary type: " + iValue);

      return Base64.getDecoder().decode(s);
    } else
      throw new IllegalArgumentException(
          "Cannot parse binary as the same type as the value (class="
              + iValue.getClass().getName()
              + "): "
              + iValue);
  }

  public static boolean startsWithIgnoreCase(final String iFirst, final String iSecond) {
    if (iFirst == null) throw new IllegalArgumentException("Origin string to compare is null");
    if (iSecond == null) throw new IllegalArgumentException("String to match is null");

    final int iSecondLength = iSecond.length();

    if (iSecondLength > iFirst.length()) return false;

    for (int i = 0; i < iSecondLength; ++i) {
      if (Character.toUpperCase(iFirst.charAt(i)) != Character.toUpperCase(iSecond.charAt(i)))
        return false;
    }
    return true;
  }

  public static int indexOf(final String iSource, final int iBegin, char... iChars) {
    if (iChars.length == 1)
      // ONE CHAR: USE JAVA INDEXOF
      return iSource.indexOf(iChars[0], iBegin);

    final int len = iSource.length();
    for (int i = iBegin; i < len; ++i) {
      for (int k = 0; k < iChars.length; ++k) {
        final char c = iSource.charAt(i);
        if (c == iChars[k]) return i;
      }
    }
    return -1;
  }

  public static int getLowerIndexOf(
      final String iText, final int iBeginOffset, final String... iToSearch) {
    int lowest = -1;
    for (String toSearch : iToSearch) {
      boolean singleQuote = false;
      boolean doubleQuote = false;
      boolean backslash = false;
      for (int i = iBeginOffset; i < iText.length(); i++) {
        if (lowest == -1 || i < lowest) {
          if (backslash && (iText.charAt(i) == '\'' || iText.charAt(i) == '"')) {
            backslash = false;
            continue;
          }
          if (iText.charAt(i) == '\\') {
            backslash = true;
            continue;
          }
          if (iText.charAt(i) == '\'' && !doubleQuote) {
            singleQuote = !singleQuote;
            continue;
          }
          if (iText.charAt(i) == '"' && !singleQuote) {
            singleQuote = !singleQuote;
            continue;
          }

          if (!singleQuote && !doubleQuote && iText.startsWith(toSearch, i)) {
            lowest = i;
          }
        }
      }
    }

    // for (String toSearch : iToSearch) {
    // int index = iText.indexOf(toSearch, iBeginOffset);
    // if (index > -1 && (lowest == -1 || index < lowest))
    // lowest = index;
    // }
    return lowest;
  }

  public static int getHigherIndexOf(
      final String iText, final int iBeginOffset, final String... iToSearch) {
    int lowest = -1;
    for (String toSearch : iToSearch) {
      int index = iText.indexOf(toSearch, iBeginOffset);
      if (index > -1 && (lowest == -1 || index > lowest)) lowest = index;
    }
    return lowest;
  }
}
