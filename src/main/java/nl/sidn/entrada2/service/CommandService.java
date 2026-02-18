package nl.sidn.entrada2.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.Command;
import nl.sidn.entrada2.messaging.Command.CommandType;
import nl.sidn.entrada2.service.StateService.APP_STATE;
import nl.sidn.entrada2.service.messaging.RequestQueue;

@Service
@Slf4j
public class CommandService {

	// aws uses 2 request queues
	@Autowired
	private List<RequestQueue> requestQueues;

	@Autowired
	private StateService stateService;

	@Autowired
	private WorkService workService;

	public void execute(Command message) {
		log.info("Received command message: {}", message);

		switch (message.getCommand()) {
		case CommandType.START -> {
			requestQueues.stream().forEach(q -> q.start());
			stateService.setState(APP_STATE.ACTIVE);
		}
		case CommandType.STOP -> {
			requestQueues.stream().forEach(q -> q.stop());
			workService.stop();
			stateService.setState(APP_STATE.STOPPED);
		}
		case CommandType.FLUSH -> {
			log.info("Flushing Iceberg writer");
			workService.flush();
		}
		default -> log.error("Unknown command, ignoring");
		}

	}

}
