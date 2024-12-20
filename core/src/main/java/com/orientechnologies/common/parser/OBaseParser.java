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
package com.orientechnologies.common.parser;

/**
 * Abstract generic command to parse.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OBaseParser {
  public String parserText;

  private transient StringBuilder parserLastWord = new StringBuilder(256);
  private transient int parserEscapeSequenceCount = 0;
  private transient int parserCurrentPos = 0;
  private transient int parserPreviousPos = 0;
  private transient char parserLastSeparator = ' ';

  public static int nextWord(
      final String iText,
      final String iTextUpperCase,
      int ioCurrentPosition,
      final StringBuilder ioWord,
      final boolean iForceUpperCase) {
    return nextWord(iText, iTextUpperCase, ioCurrentPosition, ioWord, iForceUpperCase, " =><(),");
  }

  public static int nextWord(
      final String iText,
      final String iTextUpperCase,
      int ioCurrentPosition,
      final StringBuilder ioWord,
      final boolean iForceUpperCase,
      final String iSeparatorChars) {
    ioWord.setLength(0);

    ioCurrentPosition = OStringParser.jumpWhiteSpaces(iText, ioCurrentPosition, -1);
    if (ioCurrentPosition < 0) return -1;

    getWordStatic(
        iForceUpperCase ? iTextUpperCase : iText, ioCurrentPosition, iSeparatorChars, ioWord);

    if (ioWord.length() > 0) ioCurrentPosition += ioWord.length();

    return ioCurrentPosition;
  }

  /**
   * @param iText Text where to search
   * @param iBeginIndex Begin index
   * @param iSeparatorChars Separators as a String of multiple characters
   * @param ioBuffer StringBuilder object with the word found
   */
  public static void getWordStatic(
      final CharSequence iText,
      int iBeginIndex,
      final String iSeparatorChars,
      final StringBuilder ioBuffer) {
    ioBuffer.setLength(0);

    char stringBeginChar = ' ';
    char c;

    for (int i = iBeginIndex; i < iText.length(); ++i) {
      c = iText.charAt(i);
      boolean found = false;
      for (int sepIndex = 0; sepIndex < iSeparatorChars.length(); ++sepIndex) {
        if (iSeparatorChars.charAt(sepIndex) == c) {
          // SEPARATOR AT THE BEGINNING: JUMP IT
          found = true;
          break;
        }
      }
      if (!found) break;

      iBeginIndex++;
    }

    for (int i = iBeginIndex; i < iText.length(); ++i) {
      c = iText.charAt(i);

      if (c == '\'' || c == '"' || c == '`') {
        if (stringBeginChar != ' ') {
          // CLOSE THE STRING?
          if (stringBeginChar == c) {
            // SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
            stringBeginChar = ' ';
          }
        } else {
          // START STRING
          stringBeginChar = c;
        }
      } else if (stringBeginChar == ' ') {
        for (int sepIndex = 0; sepIndex < iSeparatorChars.length(); ++sepIndex) {
          if (iSeparatorChars.charAt(sepIndex) == c && ioBuffer.length() > 0) {
            // SEPARATOR (OUTSIDE A STRING): PUSH
            return;
          }
        }
      }

      ioBuffer.append(c);
    }
  }

  public String getSyntax() {
    return "?";
  }

  /** Returns the last separator encountered, otherwise returns a blank (' '). */
  public char parserGetLastSeparator() {
    return parserLastSeparator;
  }

  /** Overwrites the last separator. To ignore it set it to blank (' '). */
  public void parserSetLastSeparator(final char iSeparator) {
    parserLastSeparator = iSeparator;
  }

  /**
   * Returns the stream position before last parsing.
   *
   * @return Offset from the beginning
   */
  public int parserGetPreviousPosition() {
    return parserPreviousPos;
  }

  /**
   * Tells if the parsing has reached the end of the content.
   *
   * @return True if is ended, otherwise false
   */
  public boolean parserIsEnded() {
    return parserCurrentPos == -1;
  }

  /**
   * Returns the current stream position.
   *
   * @return Offset from the beginning
   */
  public int parserGetCurrentPosition() {
    return parserCurrentPos;
  }

  /**
   * Returns the current character in the current stream position
   *
   * @return The current character in the current stream position. If the end is reached, then a
   *     blank (' ') is returned
   */
  public char parserGetCurrentChar() {
    if (parserCurrentPos < 0) return ' ';
    return parserText.charAt(parserCurrentPos);
  }

  /**
   * Returns the last parsed word.
   *
   * @return Last parsed word as String
   */
  public String parserGetLastWord() {
    return parserLastWord.toString();
  }

  public int getLastWordLength() {
    return parserLastWord.length() + parserEscapeSequenceCount;
  }

  /**
   * Throws a syntax error exception.
   *
   * @param iText Text about the problem.
   */
  protected abstract void throwSyntaxErrorException(final String iText);

  /**
   * Goes back to the previous position.
   *
   * @return The previous position
   */
  protected int parserGoBack() {
    parserCurrentPos = parserPreviousPos;
    return parserCurrentPos;
  }

  /**
   * Skips not valid characters like spaces and line feeds.
   *
   * @return True if the string is not ended, otherwise false
   */
  protected boolean parserSkipWhiteSpaces() {
    if (parserCurrentPos == -1) return false;

    parserCurrentPos = OStringParser.jumpWhiteSpaces(parserText, parserCurrentPos, -1);
    return parserCurrentPos > -1;
  }

  /**
   * Overwrites the current stream position.
   *
   * @param iPosition New position
   * @return True if the string is not ended, otherwise false
   */
  protected boolean parserSetCurrentPosition(final int iPosition) {
    parserCurrentPos = iPosition;
    if (parserCurrentPos >= parserText.length())
      // END OF TEXT
      parserCurrentPos = -1;
    return parserCurrentPos > -1;
  }

  /** Sets the end of text as position */
  protected void parserSetEndOfText() {
    parserCurrentPos = -1;
  }

  /**
   * Moves the current stream position forward or backward of iOffset characters
   *
   * @param iOffset Number of characters to move. Negative numbers means backwards
   * @return True if the string is not ended, otherwise false
   */
  protected boolean parserMoveCurrentPosition(final int iOffset) {
    if (parserCurrentPos < 0) return false;
    return parserSetCurrentPosition(parserCurrentPos + iOffset);
  }
}
