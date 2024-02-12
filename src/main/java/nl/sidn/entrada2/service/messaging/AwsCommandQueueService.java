package nl.sidn.entrada2.service.messaging;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.Command;
import nl.sidn.entrada2.service.CommandService;
import nl.sidn.entrada2.util.ConditionalOnAws;
import nl.sidn.entrada2.util.TimeUtil;

@ConditionalOnAws
@Service
@Slf4j
public class AwsCommandQueueService extends AbstractAwsQueue implements CommandQueue {

	@Value("${entrada.messaging.command.name}-queue")
	private String queueName;
	
	@Autowired
	private SqsTemplate sqsTemplate;
	
	@Autowired
	private CommandService commandService;
	
	/**
	 * This listener uses the command-queue as a topic by not acking the message and blocking for 1 minute.
	 * The message will return to the available mode again and will be picked up by the other entrada instances
	 */
	@SqsListener(value= "${entrada.messaging.command.name}-queue", factory = "commandSqsListenerContainerFactory",
			id="${entrada.messaging.command.name}")
	public void onMessage(Command message, Acknowledgement ack) {
		log.info("Received SQS message: {}", message);
		
		// stop listening for new file events to process
		commandService.execute(message);
		
		// sleep to allow other instances to receive the same message from the command queue
		TimeUtil.sleep(60*1000);
	}

//	@NotificationSubscriptionMapping
//	public void handleSubscriptionMessage(NotificationStatus status) {
//		// We subscribe to start receive the message
//		status.confirmSubscription();
//	}

//	@NotificationMessageMapping
//	public void handleNotificationMessage(@NotificationSubject String subject, @NotificationMessage String message) {
//		log.info("Received SNS command message: {}", message);
//		
//		//commandService.execute(message);
//	}
//
//	@NotificationUnsubscribeConfirmationMapping
//	public void handleUnsubscribeMessage(NotificationStatus status) {
//		// e.g. the client has been unsubscribed and we want to "re-subscribe"
//		status.confirmSubscription();
//	}

	// @SqsListener("${entrada.messaging.command-queue}-queue.fifo")
//	public void receiveMessage(Command message) {
//
//		commandService.execute(message);
//	}

	public void send(Command message) {
		log.info("Sending Message to the Queue : " + message);

		// send command to pub/sub queue, this allows all entrada instances to
		// receive the command, including this instance.
		sqsTemplate.send(name(), message);
	}

	@Override
	public String name() {
		return queueName;
	}

}
