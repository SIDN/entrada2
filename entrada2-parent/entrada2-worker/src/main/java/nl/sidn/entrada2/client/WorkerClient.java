package nl.sidn.entrada2.client;

import org.springframework.cloud.openfeign.FeignClient;
import nl.sidn.entrada2.api.BaseWork;

@FeignClient(name="workerClient")
public interface WorkerClient extends BaseWork {

}
