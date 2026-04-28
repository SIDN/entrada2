package nl.sidn.entrada2.schedule;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.config.EntradaS3Properties;
import nl.sidn.entrada2.messaging.S3EventNotification;
import nl.sidn.entrada2.messaging.S3EventNotification.S3Entity;
import nl.sidn.entrada2.messaging.S3EventNotification.S3EventNotificationRecord;
import nl.sidn.entrada2.service.LeaderService;
import nl.sidn.entrada2.service.S3Service;
import nl.sidn.entrada2.util.S3ObjectTagName;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * This class is responsible for checking for new objects in the configured s3 bucket and prefixes, it does this by listing all objects and checking if they have a specific tag, if not it adds the tag and sends a message to the request queue
 * The tag is used to prevent multiple instances of Entrada from processing the same object when running in a clustered environment
 * The class is only active when the property "entrada.schedule.new-object-secs" is set to a value greater than 0
 * 
 * This is only used as a fallback for when S3 event notifications are not available, it is recommended to use S3 event notifications instead of this class for better performance and reliability
 * 
 */
@Slf4j
@ConditionalOnExpression(
	    "T(org.apache.commons.lang3.StringUtils).isNotEmpty('${entrada.schedule.new-object-secs}')"
	)
@Component
public class NewObjectChecker {

	@Autowired
	private EntradaS3Properties s3Properties;

	@Autowired
	private S3Service s3Service;
	
	@Autowired
	private LeaderService leaderService;
	
	@Value("${entrada.messaging.request.name}")
	private String requestQueue;

	// should match fastClient maxConnections for maximum throughput
	@Value("${entrada.schedule.new-object-threads:50}")
	private int tagThreads;

	@Autowired(required = false)
	@Qualifier("rabbitJsonTemplate")
	private AmqpTemplate rabbitTemplate;
	
	private final AtomicBoolean isRunning = new AtomicBoolean(false);
	
	@Scheduled(initialDelay = 5000, fixedDelayString = "#{${entrada.schedule.new-object-secs:120}*1000}")
	public void execute() {
		if (!leaderService.isleader()) {
			// only leader is allowed to continue
			return;
		}

		boolean acquired = isRunning.compareAndSet(false, true);
		log.debug("Execute called, thread: {}, isRunning before: {}, acquired: {}", 
				Thread.currentThread().getName(), !acquired, acquired);
		
		if (!acquired) {
			log.warn("Skipping execution, previous run still in progress");
			return;
		}
		
		// find new objects
		log.info("Start checking for new objects");

		try {
			for(String prefix: s3Properties.getPcapInPrefixes()) {
				log.info("Start checking for new objects with prefix: {}", prefix);
				int counterAdded = scanForNewObjects(prefix);
				log.info("Detected {} new object(s) for prefix: {}", counterAdded, prefix);
			}
		} catch (Exception e) {
			log.error("Unexpected exception while scanning for new objects");
		} finally {
			isRunning.set(false);
		}
	}

	public int scanForNewObjects(String prefix) {

		List<S3Object> s3Objects = s3Service.ls(s3Properties.getBucket(), StringUtils.appendIfMissing(prefix, "/"));
		s3Objects.sort(Comparator.comparing(S3Object::key));

		AtomicInteger counterAdded = new AtomicInteger(0);
		ExecutorService executor = Executors.newFixedThreadPool(tagThreads);
		List<Future<?>> futures = new ArrayList<>(s3Objects.size());

		for (S3Object s3Object : s3Objects) {
			futures.add(executor.submit(() -> {
				Map<String, String> tags = new ConcurrentHashMap<>();
				if (s3Service.tags(s3Properties.getBucket(), s3Object.key(), tags)) {
					if (StringUtils.isEmpty(tags.get(S3ObjectTagName.ENTRADA_OBJECT_DETECTED.value))) {
						log.info("New object found: {}/{}", s3Properties.getBucket(), s3Object.key());
	
						tags.put(S3ObjectTagName.ENTRADA_OBJECT_DETECTED.value, "true");
						if (s3Service.tag(s3Properties.getBucket(), s3Object.key(), tags)) {
							rabbitTemplate.convertAndSend(requestQueue + "-exchange", requestQueue,
									createEvent(s3Properties.getBucket(), s3Object.key()));
							counterAdded.incrementAndGet();
						} else {
							log.error("Failed to set tags on newly detected object: {}", s3Object.key());
						}
					}
				}
			}));
		}

		executor.shutdown();
		for (Future<?> f : futures) {
			try { f.get(); } catch (Exception e) {
				log.error("Error processing object tag check", e);
			}
		}

		return counterAdded.get();
	}
	
	private S3EventNotification createEvent(String bucket, String key) {
		 new S3EventNotification.S3BucketEntity(bucket, null, null);
			S3Entity e = new S3Entity(null, new S3EventNotification.S3BucketEntity(s3Properties.getBucket(), null, null),
					new S3EventNotification.S3ObjectEntity(key, null, null, null, null), null);
			
			List<S3EventNotificationRecord> records = List.of(new S3EventNotificationRecord(null, "s3:ObjectCreated:Put", null, null, null, null, null, e, null ,null,null, null, null));
			
			return new S3EventNotification(records);

	}

}
