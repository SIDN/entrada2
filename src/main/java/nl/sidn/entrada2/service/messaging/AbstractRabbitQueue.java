package nl.sidn.entrada2.service.messaging;

import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.Command;
import nl.sidn.entrada2.messaging.S3EventNotification;
import nl.sidn.entrada2.messaging.Command.CommandType;
import nl.sidn.entrada2.service.StateService;
import nl.sidn.entrada2.service.WorkService;
import nl.sidn.entrada2.service.StateService.APP_STATE;

@Slf4j
public abstract class AbstractRabbitQueue implements QueueService{
	
//	@Value("${spring.cloud.aws.sqs.enabled}")
//	protected boolean awsEnabled;

	//@Autowired
	//private AwsRequestQueueService requestQueueService;
//	@Autowired
//	private StateService stateService;
//	@Autowired
//	private WorkService workService;
	
	//@Autowired(required = false)
	//private SqsMessageListenerContainer<S3EventNotification> sqsMessageListenerContainer;

	@Autowired(required = false)
	private RabbitListenerEndpointRegistry listenerRegistry;


	@Override
	public void start() {
		//if (awsEnabled) {
		//	sqsMessageListenerContainer.start();
		//} else {
			listenerRegistry.getListenerContainer(name()).start();
		//}
	}

	@Override
	public void stop() {
		//if (awsEnabled) {
			//sqsMessageListenerContainer.stop();
		//} else {
			listenerRegistry.getListenerContainer(name()).stop();
		//}
	}
	
//	protected void handleMessage(Command message) {
//
//		if(message.getCommand() == CommandType.START) {
//			requestQueueService.start();
//			stateService.setState(APP_STATE.RUNNING);
//		}else if(message.getCommand() == CommandType.STOP) {
//			requestQueueService.stop();
//			workService.stop();
//			stateService.setState(APP_STATE.STOPPED);
//		}else {
//			log.error("Unknown command, ignoring");
//		}
//	}
	
	//public abstract String name();
}
