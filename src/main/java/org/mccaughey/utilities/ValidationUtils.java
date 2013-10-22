package org.mccaughey.utilities;

public final class ValidationUtils {

  private ValidationUtils() {
  }

  public static boolean isValidDouble(Double zScore) {
    return !(zScore.equals(Double.NaN)
        || zScore.equals(Double.NEGATIVE_INFINITY) || zScore
          .equals(Double.POSITIVE_INFINITY));
  }
}
