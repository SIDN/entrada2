package nl.sidn.entrada2.worker.client;

import org.springframework.cloud.openfeign.FeignClient;

import nl.sidn.entrada.api.BaseCommand;
import nl.sidn.entrada.api.BaseWork;

@FeignClient(name="workerClient")
public interface WorkerClient extends BaseWork {
	


}
