package nl.sidn.entrada2.config;

import java.util.concurrent.Executor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class ThreadConfig {
  
  @Bean
  Executor taskExecutor() {
      ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
      executor.setCorePoolSize(10);
      executor.setMaxPoolSize(10);
      executor.setQueueCapacity(500);
      executor.setThreadNamePrefix("ENTRADA-worker-");
      executor.initialize();
      return executor;
  }

}
