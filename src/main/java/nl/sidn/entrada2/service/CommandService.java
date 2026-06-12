package nl.sidn.entrada2.service;

import java.util.List;

import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.Command;
import nl.sidn.entrada2.messaging.Command.CommandType;
import nl.sidn.entrada2.schedule.ExpiredObjectChecker;
import nl.sidn.entrada2.schedule.NewObjectChecker;
import nl.sidn.entrada2.service.StateService.APP_STATE;
import nl.sidn.entrada2.service.messaging.RequestQueue;

@Service
@Slf4j
public class CommandService {

private final List<RequestQueue> requestQueues;
        private final StateService stateService;
        private final WorkService workService;
        private final NewObjectChecker newObjectChecker;
        private final ExpiredObjectChecker expiredObjectChecker;

        public CommandService(List<RequestQueue> requestQueues, StateService stateService, WorkService workService, NewObjectChecker newObjectChecker, ExpiredObjectChecker expiredObjectChecker) {
                this.requestQueues = requestQueues;
                this.stateService = stateService;
                this.workService = workService;
                this.newObjectChecker = newObjectChecker;
                this.expiredObjectChecker = expiredObjectChecker;
        }

	public void execute(Command message) {
		log.info("Received command message: {}", message);

		switch (message.getCommand()) {
		case CommandType.START -> {
			requestQueues.stream().forEach(q -> q.start());
			stateService.setState(APP_STATE.ACTIVE);
			newObjectChecker.start();
			expiredObjectChecker.start();
		}
		case CommandType.STOP -> {
			requestQueues.stream().forEach(q -> q.stop());
			workService.stop();
			stateService.setState(APP_STATE.STOPPED);
			newObjectChecker.stop();
			expiredObjectChecker.stop();
		}
		case CommandType.FLUSH -> {
			log.info("Flushing Iceberg writer");
			workService.flush();
		}
		default -> log.error("Unknown command, ignoring");
		}

	}

}
