package nl.sidn.entrada2.service.messaging;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.extern.slf4j.Slf4j;
import io.awspring.cloud.sqs.listener.MessageListenerContainerRegistry;

import nl.sidn.entrada2.messaging.RequestMessage;
import nl.sidn.entrada2.service.WorkService;
import nl.sidn.entrada2.util.ConditionalOnAws;

@ConditionalOnAws
@Service
@Slf4j
public class AwsRequestQueueService extends AbstractAwsQueue implements RequestQueue {
	

	@Value("${entrada.messaging.request.name}-queue.fifo")
	private String requestQueue;
	private final WorkService workService;

	public AwsRequestQueueService(MessageListenerContainerRegistry listenerRegistry, WorkService workService) {
		super(listenerRegistry);
		this.workService = workService;
	}

	@SqsListener(value = "${entrada.messaging.request.name}-queue.fifo", id="${entrada.messaging.request.name}-queue.fifo")
	public void receiveMessage(RequestMessage message) {
		log.info("Received SQS message: {}", message);
		
		process(message.getBucket(), message.getKey());
	}

	
	private void process(String bucket, String key) {
		
		workService.process(bucket, key);
	}


	@Override
	public String name() {
		return requestQueue;
	}
	


}
