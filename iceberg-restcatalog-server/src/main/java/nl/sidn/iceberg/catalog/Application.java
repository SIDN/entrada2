package nl.sidn.iceberg.catalog;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication(exclude = {SecurityAutoConfiguration.class, UserDetailsServiceAutoConfiguration.class})
public class Application {
  
//  @Autowired
//  private RESTCatalogServer server;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

 // @EventListener
//  public void onApplicationEvent(ContextRefreshedEvent event) {
//    log.info("Application initialized, start server");
//    server.start();
//  }

}
