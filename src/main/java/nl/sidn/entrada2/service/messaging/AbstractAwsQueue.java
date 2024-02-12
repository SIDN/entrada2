package nl.sidn.entrada2.service.messaging;

import org.springframework.beans.factory.annotation.Autowired;

import io.awspring.cloud.sqs.listener.MessageListenerContainerRegistry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractAwsQueue implements QueueService{

	@Autowired
	private MessageListenerContainerRegistry listenerRegistry;

	@Override
	public void start() {
		log.info("Starting queue: {}", name());
		
		listenerRegistry.getContainerById(name()).start();
	}

	@Override
	public void stop() {
		log.info("Stopping queue: {}", name());
		
		listenerRegistry.getContainerById(name()).stop();
	}

}
