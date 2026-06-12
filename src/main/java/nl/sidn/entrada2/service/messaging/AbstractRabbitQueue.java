package nl.sidn.entrada2.service.messaging;

import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.lang.Nullable;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRabbitQueue implements Queue {

	private final RabbitListenerEndpointRegistry listenerRegistry;

	protected AbstractRabbitQueue(@Nullable RabbitListenerEndpointRegistry listenerRegistry) {
		this.listenerRegistry = listenerRegistry;
	}

	@Override
	public void start() {
		log.info("Starting queue: {}", name());
		listenerRegistry.getListenerContainer(name()).start();
	}

	@Override
	public void stop() {
		log.info("Stopping queue: {}", name());
		listenerRegistry.getListenerContainer(name()).stop();
	}

}
