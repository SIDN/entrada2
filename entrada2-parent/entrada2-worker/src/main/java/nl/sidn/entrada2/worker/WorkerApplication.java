package nl.sidn.entrada2.worker;


import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@EnableFeignClients
public class WorkerApplication {
//	
//	@Autowired
//	private WorkerClient workerClient;
	
	@Value("${spring.application.name}")
	private String appName;
	 
	//private static String id;

	public static void main(String[] args) {
		
	//	UUID randomUUID = UUID.randomUUID();

	  //  id = randomUUID.toString();
		
		SpringApplication.run(WorkerApplication.class, args);
	}

//	
//	@Scheduled(fixedRate = 3000)
//	public void getStatus() {
//		System.out.println("calling svr");
//		
//		System.out.println("Received from svr: " + controllerClient.getStatus(id));
//	}
	
//	@Scheduled(fixedRate = 3000)
//	public void getWork() {
//		System.out.println("calling svr");
//		
//		System.out.println("Received from svr: " + workerClient.work());
//	}
	

}
