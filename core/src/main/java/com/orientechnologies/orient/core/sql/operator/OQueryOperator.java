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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.List;

/**
 * Query Operators. Remember to handle the operator in OQueryItemCondition.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OQueryOperator {

  public static enum ORDER {
    /** Used when order compared to other operator cannot be evaluated or has no consequences. */
    UNKNOWNED,
    /** Used when this operator must be before the other one */
    BEFORE,
    /** Used when this operator must be after the other one */
    AFTER,
    /** Used when this operator is equal the other one */
    EQUAL
  }

  /**
   * Default operator order. can be used by additional operator to locate themself relatively to
   * default ones.
   *
   * <p>WARNING: ORDER IS IMPORTANT TO AVOID SUB-STRING LIKE "IS" and AND "INSTANCEOF": INSTANCEOF
   * MUST BE PLACED BEFORE! AND ALSO FOR PERFORMANCE (MOST USED BEFORE)
   */
  protected static final Class<?>[] DEFAULT_OPERATORS_ORDER = {};

  public final String keyword;
  public final int precedence;
  public final int expectedRightWords;
  public final boolean unary;
  public final boolean expectsParameters;

  protected OQueryOperator(final String iKeyword, final int iPrecedence, final boolean iUnary) {
    this(iKeyword, iPrecedence, iUnary, 1, false);
  }

  protected OQueryOperator(
      final String iKeyword,
      final int iPrecedence,
      final boolean iUnary,
      final int iExpectedRightWords) {
    this(iKeyword, iPrecedence, iUnary, iExpectedRightWords, false);
  }

  protected OQueryOperator(
      final String iKeyword,
      final int iPrecedence,
      final boolean iUnary,
      final int iExpectedRightWords,
      final boolean iExpectsParameters) {
    keyword = iKeyword;
    precedence = iPrecedence;
    unary = iUnary;
    expectedRightWords = iExpectedRightWords;
    expectsParameters = iExpectsParameters;
  }

  @Override
  public String toString() {
    return keyword;
  }

  /**
   * Default State-less implementation: does not save parameters and just return itself
   *
   * @param iParams
   * @return
   */
  public OQueryOperator configure(final List<String> iParams) {
    return this;
  }

  public String getSyntax() {
    return "<left> " + keyword + " <right>";
  }

  public boolean isUnary() {
    return unary;
  }

  /**
   * Check priority of this operator compare to given operator.
   *
   * @param other
   * @return ORDER place of this operator compared to given operator
   */
  public ORDER compare(OQueryOperator other) {
    final Class<?> thisClass = this.getClass();
    final Class<?> otherClass = other.getClass();

    int thisPosition = -1;
    int otherPosition = -1;
    for (int i = 0; i < DEFAULT_OPERATORS_ORDER.length; i++) {
      // subclass of default operators inherit their parent ordering
      final Class<?> clazz = DEFAULT_OPERATORS_ORDER[i];
      if (clazz.isAssignableFrom(thisClass)) {
        thisPosition = i;
      }
      if (clazz.isAssignableFrom(otherClass)) {
        otherPosition = i;
      }
    }

    if (thisPosition == -1 || otherPosition == -1) {
      // cannot decide which comes first
      return ORDER.UNKNOWNED;
    }

    if (thisPosition > otherPosition) {
      return ORDER.AFTER;
    } else if (thisPosition < otherPosition) {
      return ORDER.BEFORE;
    }

    return ORDER.EQUAL;
  }

  public boolean canShortCircuit(Object l) {
    return false;
  }

  public boolean isSupportingBinaryEvaluate() {
    return false;
  }

  public String getKeyword() {
    return keyword;
  }

  public boolean evaluate(Object iLeft, Object iRight, OCommandContext ctx) {
    throw new UnsupportedOperationException();
  }
}
