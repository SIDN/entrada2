package nl.sidn.entrada2.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;


public class TimeUtil {

  private final static ZoneId UTC = ZoneId.of("UTC");

  private TimeUtil() {}

  public static LocalDateTime timestampFromMillis(long millis) {

    return Instant
        .ofEpochMilli(millis)
        .atZone(UTC)
        .toLocalDateTime();

    // return LocalDateTime.ofEpochSecond(
    // TimeUnit.MILLISECONDS.toSeconds(micros),
    // (int) TimeUnit.MICROSECONDS.toNanos(micros % 1000),
    // ZoneOffset.UTC);
  }

}
