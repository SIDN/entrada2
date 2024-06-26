package nl.sidn.entrada2.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TimeUtil {
  private TimeUtil() {}
  
  private final static ZoneId UTC = ZoneId.of("UTC");
  
  public static LocalDateTime timestampFromMillis(long millis) {

    return Instant
        .ofEpochMilli(millis)
        .atZone(UTC)
        .toLocalDateTime();
  }


  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      log.error("Interupted while having a nice sleep", e);
    }
  }

}
