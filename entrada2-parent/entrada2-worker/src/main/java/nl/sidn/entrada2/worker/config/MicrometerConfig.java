package nl.sidn.entrada2.worker.config;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import io.micrometer.core.instrument.MeterRegistry;

@Configuration
public class MicrometerConfig {

  @Bean
  public MeterRegistryCustomizer<MeterRegistry> commonTags(
      @Value("${management.graphite.metrics.export.prefix:entrada}") String prefix) {
    return r -> r.config().commonTags("prefix", StringUtils.trim(prefix));
  }

}
