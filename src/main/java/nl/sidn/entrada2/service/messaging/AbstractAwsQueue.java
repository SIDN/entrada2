package nl.sidn.entrada2.service.messaging;

import org.springframework.beans.factory.annotation.Autowired;

import io.awspring.cloud.sqs.listener.MessageListenerContainerRegistry;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.S3EventNotification;
import nl.sidn.entrada2.messaging.SqsEventMessage;

@Slf4j
public abstract class AbstractAwsQueue implements QueueService{
	
//	@Value("${spring.cloud.aws.sqs.enabled}")
//	protected boolean awsEnabled;
//
//	@Autowired
//	private AwsRequestQueueService requestQueueService;
//	@Autowired
//	private StateService stateService;
//	@Autowired
//	private WorkService workService;
//	
	@Autowired
	private MessageListenerContainerRegistry listenerRegistry;

//	@Autowired(required = false)
//	private RabbitListenerEndpointRegistry listenerRegistry;


	@Override
	public void start() {
		log.info("Starting queue: {}", name());
		
		listenerRegistry.getContainerById(name()).start();
		
		//if (awsEnabled) {
		//	sqsMessageListenerContainer.start();
//		} else {
//			listenerRegistry.getListenerContainer(name()).start();
//		}
	}

	@Override
	public void stop() {
		log.info("Stopping queue: {}", name());
		
		listenerRegistry.getContainerById(name()).stop();
		
//		if (awsEnabled) {
//			//sqsMessageListenerContainer.stop();
//		} else {
//			listenerRegistry.getListenerContainer(name()).stop();
//		}
	}

}
