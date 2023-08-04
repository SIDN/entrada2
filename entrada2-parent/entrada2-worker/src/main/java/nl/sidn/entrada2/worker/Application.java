package nl.sidn.entrada2.worker;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableFeignClients
@EnableAsync
public class Application {

  @Value("${spring.application.name}")
  private String appName;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }


}
