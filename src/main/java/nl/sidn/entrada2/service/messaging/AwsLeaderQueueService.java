package nl.sidn.entrada2.service.messaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.zip.Deflater;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.iceberg.DataFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.IcebergService;
import nl.sidn.entrada2.service.LeaderService;
import nl.sidn.entrada2.util.CompressionUtil;
import nl.sidn.entrada2.util.ConditionalOnAws;
import nl.sidn.entrada2.util.UrlUtil;

@ConditionalOnAws
@Service
@Slf4j
public class AwsLeaderQueueService extends AbstractAwsQueue implements LeaderQueue {

	@Value("${entrada.messaging.leader.name}-queue.fifo")
	private String queueName;

	@Autowired
	//@Qualifier("byteTemplate")
	private SqsTemplate sqsTemplate;
	

	@Autowired
	private IcebergService icebergService;

	@Autowired
	private LeaderService leaderService;

	@SqsListener(value="${entrada.messaging.leader.name}-queue.fifo",
			id="${entrada.messaging.leader.name}-queue.fifo",
			factory = "leaderSqsListenerContainerFactory")
	public void onMessage(String message) {
		//log.info("Received SQS message, rows: {} path: {}", message);
		
		if (leaderService.isleader()) {
			DataFile df = null;
			
			try {
				byte[] data = CompressionUtil.decompress(Base64.getUrlDecoder().decode(message));
				df = (DataFile) org.springframework.amqp.utils.SerializationUtils.deserialize(data);
				
				log.info("Received new file to commit to leader queue, file : " + df.path());
				
			} catch (Exception e) {
				throw new RuntimeException("Converting message failed", e);
			}
			
			icebergService.commit(df);
		}else {
			
			log.error("Received message for leader when not being the leader, the leader queue listener is not shut down properly");
			
		}
	}

	public void send(DataFile message) {
		log.info("Send new file to commit to leader queue, file : " + message.path());

		String encodedString = "";
		try {
			byte[] data = org.springframework.amqp.utils.SerializationUtils.serialize(message);
			log.info("Size message before compression: {}", data.length);
			
			byte[] compressed = CompressionUtil.compress(data);
			log.info("Size message after compression: {}", compressed.length);
			
			encodedString = Base64.getUrlEncoder().encodeToString(compressed);
			
		} catch (Exception e) {
			throw new RuntimeException("Converting message failed", e);
		}
		
		log.info("size after url-encoded: " + encodedString.getBytes().length);
		sqsTemplate.send(name(), encodedString);
	}
	
//	public String toString(DataFile datafile) {
//	  final DatumWriter<DataFile> evaluatorWriter = new SpecificDatumWriter<>(DataFile.class);
//	  try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
//	    final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(datafile..getSchema(), out);
//	    evaluatorWriter.write(datafile, encoder);
//	    encoder.flush();
//	    return out.toString(Charset.defaultCharset());
//	  } catch (final IOException e) {
//	    throw new RuntimeException(e);
//	  }
//	}

	@Override
	public String name() {
		return queueName;
	}

}
