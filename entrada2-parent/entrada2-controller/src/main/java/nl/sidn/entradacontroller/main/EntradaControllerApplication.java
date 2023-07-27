package nl.sidn.entradacontroller.main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableDiscoveryClient
@Slf4j
public class EntradaControllerApplication implements CotrollerApi  {
	
	@Value("${test.var}")
    private String testCfg;

    @Value("${spring.application.name}")
    private String appName;
//    
//    @Autowired
//    private DiscoveryClient discoveryClient;

    public static void main(String[] args) {
        SpringApplication.run(EntradaControllerApplication.class, args);
    }

    @Override
	public String getStatus(String name) {
		log.info("Received status request from: {} ", name);
		return "status test";
	}

	@Override
	public String getWork(String name) {
		log.info("Received work request from: {} ", name);
		return "work test";
	}
	


}