package nl.sidn.entrada2.service;

import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import nl.sidn.entrada2.messaging.S3EventNotification;

public abstract class AbstractQueueService implements QueueService{
	
	@Value("${spring.cloud.aws.sqs.enabled}")
	protected boolean awsEnabled;
	
	@Autowired(required = false)
	private SqsMessageListenerContainer<S3EventNotification> sqsMessageListenerContainer;

	@Autowired(required = false)
	private RabbitListenerEndpointRegistry listenerRegistry;


	@Override
	public void start() {
		if (awsEnabled) {
			sqsMessageListenerContainer.start();
		} else {
			listenerRegistry.getListenerContainer(name()).start();
		}
	}

	@Override
	public void stop() {
		if (awsEnabled) {
			sqsMessageListenerContainer.stop();
		} else {
			listenerRegistry.getListenerContainer(name()).stop();
		}
	}
	
	public abstract String name();
}
