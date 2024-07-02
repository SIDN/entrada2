package nl.sidn.entrada2.util;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TimeUtil {
  private TimeUtil() {}

  public static OffsetDateTime timestampFromMillis(long millis) {
	  
    return Instant
        .ofEpochMilli(millis)
        .atOffset(ZoneOffset.UTC);
  }


  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      log.error("Interupted while having a nice sleep", e);
    }
  }

}
