package nl.sidn.entradacontroller.main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ApplicationListener {

    @EventListener
    public void handle(ContextStartedEvent event) {
        log.info("ContextStartedEvent annotation based listener raised");
     

    }

    @EventListener
    public void handle(ContextRefreshedEvent event) {
        log.info("ContextRefreshedEvent annotation based listener raised");
        try {
     			testWrite();
     		} catch (IOException e) {
     			// TODO Auto-generated catch block
     			e.printStackTrace();
     		}

    }
    
	public void testWrite() 
			  throws IOException {
			    String str = "Hello";
			    BufferedWriter writer = new BufferedWriter(new FileWriter("/data/entrada/write-test"));
			    writer.write(str);
			    
			    writer.close();
			}
}