package nl.sidn.entrada.api;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;


public interface BaseCommand {
	
	@RequestMapping(method = RequestMethod.GET, value = "/status/{name}")
    String getStatus(@PathVariable("name") String name);
	
	@RequestMapping(method = RequestMethod.GET, value = "/work/{name}")
    String getWork(@PathVariable("name") String name);

}
