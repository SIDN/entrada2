package nl.sidn.entrada2.schedule;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.config.EntradaS3Properties;
import nl.sidn.entrada2.service.LeaderService;
import nl.sidn.entrada2.service.S3Service;
import nl.sidn.entrada2.util.CompressionUtil;
import nl.sidn.entrada2.util.S3ObjectTagName;
import software.amazon.awssdk.services.s3.model.S3Object;

@Slf4j
@Component
public class ExpiredObjectChecker {

	@Value("${entrada.autostart:true}")
	private boolean running;

	private final LeaderService leaderService;
	private final EntradaS3Properties s3Properties;
	private final S3Service s3Service;

	@Value("${entrada.object.max-wait-time-secs:7200}")
	private int maxWaitTime;

	@Value("${entrada.object.max-proc-time-secs:3600}")
	private int maxProcTime;

	// max lifetime (in seconds) of a claim/lock marker object (see S3Service#claim), default
	// 30 minutes. If a lock is still present after this time (e.g. because the pod that
	// created it crashed before releasing it) it is removed so the object can be claimed and
	// processed again.
	@Value("${entrada.object.lock-max-lifetime-secs:1800}")
	private int maxLockLifetime;

	public ExpiredObjectChecker(LeaderService leaderService, EntradaS3Properties s3Properties, S3Service s3Service) {
		this.leaderService = leaderService;
		this.s3Properties = s3Properties;
		this.s3Service = s3Service;
	}

	@Scheduled(initialDelayString = "60s", fixedDelayString = "#{'${entrada.schedule.expired-object-min:10}'.trim() + 'm'}")
	public void execute() {
		log.info("ExpiredObjectChecker execute called");

		if(!running) {
			log.info("ExpiredObjectChecker is not running, skipping execution");
			return;
		}

		if (leaderService.isleader()) {
			// only leader is allowed to continue
			try {
				for (String prefix : s3Properties.getPcapInPrefixes()) {
					log.info("Start checking for expired objects with prefix: {}", prefix);
					checkForExpiredObjects(prefix);
				}

				log.info("Start checking for expired/orphaned lock objects");
				checkForExpiredLocks();
			} catch (Exception e) {
				log.error("Unexpected exception while checking for expired objects");
			}
		}

		log.info("ExpiredObjectChecker execute done");
	}

	public void checkForExpiredObjects(String prefix) {
		LocalDateTime now = LocalDateTime.now();
		ZoneId localZone = ZoneId.systemDefault();

		int counterIncomplete = 0;
		int counterNotPickedUp = 0;

		List<S3Object> objects = s3Service.ls(s3Properties.getBucket(), StringUtils.appendIfMissing(prefix, "/"));

		for (S3Object obj : objects) {

			if (!CompressionUtil.isSupportedFormat(obj.key())) {
				// ignore unknown types
				continue;
			}
			Map<String, String> tags = new HashMap<>();

			if (s3Service.tags(s3Properties.getBucket(), obj.key(), tags)) {
				if (tags.containsKey(S3ObjectTagName.ENTRADA_OBJECT_MAX_TRIES_REACHED.value)) {
					// ingore objects that have already reached max tries, they will not be processed anymore
					log.debug("Object {} has already reached max tries, ignoring it for expired object check", obj.key());
					continue;
				} else if (tags.containsKey(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value)) { // check for ts_start without ts_end
					if (!tags.containsKey(S3ObjectTagName.ENTRADA_PROCESS_TS_END.value)) {
						// check if object claim is expired
						Optional<LocalDateTime> startDate = stringToDate(
								tags.get(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value));
						LocalDateTime max = startDate.get().plusSeconds(maxProcTime);
						log.info("Check if {} with start date {} and max date {} is before end date {}", obj.key(),
								startDate.get(), max, now);

						if (max.isBefore(now)) {
							Integer value = NumberUtils.createInteger(tags.get(S3ObjectTagName.ENTRADA_OBJECT_TRIES.value));
							int tries = 1;
							if(value != null) {
								tries = tries + value.intValue();
							} 
						
							// expired object and max tries not yet reached, remove start tag
							// this will cause the object to be processed again
							log.info("Object not processed correctly, doing retry for: {}", obj.key());

							tags.remove(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value);
							// remove detected just in case
							tags.remove(S3ObjectTagName.ENTRADA_OBJECT_DETECTED.value);
							tags.put(S3ObjectTagName.ENTRADA_OBJECT_TRIES.value, String.valueOf(tries));
							s3Service.tag(s3Properties.getBucket(), obj.key(), tags);

							counterIncomplete++;							
						}
					}
				} else if (tags.containsKey(S3ObjectTagName.ENTRADA_OBJECT_DETECTED.value)) {
					LocalDateTime objDate = LocalDateTime.ofInstant(obj.lastModified(), localZone);
					if (objDate.plusSeconds(maxWaitTime).isBefore(now)) {
						// object has not yet been picked up by a worker, maybe queue was unavailable
						// resend it to the queue to make sure it will be processed

						// setting the tags to the object again, will cause an
						// s3:ObjectCreated:PutTagging event to be sent to the queue
						log.info("Object {} was not picked up, resending it to queue for processing", obj.key());
						tags.put(S3ObjectTagName.ENTRADA_WAIT_EXPIRED.value, "true");
						// remove detected, this will cause renew checker to also add it to queue again
						// if not using events
						tags.remove(S3ObjectTagName.ENTRADA_OBJECT_DETECTED.value);
						s3Service.tag(s3Properties.getBucket(), obj.key(), tags);

						counterNotPickedUp++;
					}
				}
			}

		}

		log.info("Found {} incomplete object(s) and {} not picked up objects", counterIncomplete,
				counterNotPickedUp);
	}

	/**
	 * Remove claim/lock marker objects (see {@link S3Service#claim(String, String)}) that are
	 * older than {@code entrada.object.lock-max-lifetime-secs}. A lock this old means the
	 * instance that created it never released it, most likely because the pod crashed or was
	 * killed mid-processing. Without this cleanup the object would remain claimed forever and
	 * never be retried.
	 */
	public void checkForExpiredLocks() {
		LocalDateTime now = LocalDateTime.now();
		ZoneId localZone = ZoneId.systemDefault();

		int counterExpiredLocks = 0;

		List<S3Object> locks = s3Service.ls(s3Properties.getBucket(), S3Service.LOCK_PREFIX);

		for (S3Object lock : locks) {
			LocalDateTime lockDate = LocalDateTime.ofInstant(lock.lastModified(), localZone);
			if (lockDate.plusSeconds(maxLockLifetime).isBefore(now)) {
				log.warn("Lock {} is older than max lifetime ({}s), removing it so the object can be claimed again",
						lock.key(), maxLockLifetime);
				s3Service.delete(s3Properties.getBucket(), lock.key());
				counterExpiredLocks++;
			}
		}

		log.info("Found and removed {} expired/orphaned lock object(s)", counterExpiredLocks);
	}

	private Optional<LocalDateTime> stringToDate(String datetr) {
		try {
			return Optional.of(LocalDateTime.parse(datetr, DateTimeFormatter.ISO_DATE_TIME));
		} catch (Exception e) {
			log.error("Error parsing date string: {}, error: {}", datetr);
			// ignore error
		}
		return Optional.empty();
	}

	public void stop() {
		this.running = false;
	}

	public void start() {
		this.running = true;
	}

}
