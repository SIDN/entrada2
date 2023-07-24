package nl.sidn.entradaworker.main;

import org.springframework.cloud.openfeign.FeignClient;

import nl.sidn.entrada.api.BaseCommand;

@FeignClient("entrada-controller-service")
public interface ControllerClient extends BaseCommand {
	


}
