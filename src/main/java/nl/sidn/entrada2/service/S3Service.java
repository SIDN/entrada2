package nl.sidn.entrada2.service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

@Service
@Slf4j
public class S3Service {

	@Autowired
	private S3Client s3Client;

	public Optional<InputStream> read(String bucket, String key) {

		GetObjectRequest objectRequest = GetObjectRequest.builder().key(key).bucket(bucket).build();

		try {
			return Optional.of(s3Client.getObject(objectRequest));
		} catch (Exception e) {
			log.error("Error file getting {} from bucket {}", key, bucket, e);
			return Optional.empty();
		}
	}

	public Optional<String> readObectAsString(String bucket, String key) {

		try (InputStream is = read(bucket, key).get()) {
			return Optional.of(StreamUtils.copyToString(is, StandardCharsets.UTF_8));

		} catch (Exception e) {
			log.error("Error file getting {} from bucket {}", key, bucket, e);
		}
		return Optional.empty();
	}

	public boolean write(String bucket, String key, String content) {
		log.info("Save file: {}", key);

		PutObjectRequest putOb = PutObjectRequest.builder().bucket(bucket).key(key).build();

		try {
			s3Client.putObject(putOb, RequestBody.fromString(content));
		} catch (Exception e) {
			log.error("Write error", e);
			return false;
		}

		return true;
	}

	public boolean write(InputStream is, String bucket, String key) {
		log.info("Save file: {}", key);

		try {

			log.info("Size of file: {} bytes", is.available());

			PutObjectRequest putOb = PutObjectRequest.builder().bucket(bucket).key(key).build();
			s3Client.putObject(putOb, RequestBody.fromInputStream(is, is.available()));

		} catch (Exception e) {
			log.error("Write error", e);
			return false;
		}

		return true;
	}

	public List<Pair<String, Instant>> ls(String bucket, String key) {

		ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucket).prefix(key).build();

		try {
			ListObjectsResponse res = s3Client.listObjects(listObjects);

			return res.contents().stream().map(o -> Pair.of(o.key(), o.lastModified())).collect(Collectors.toList());

		} catch (Exception e) {
			log.error("Read error", e);
		}

		return Collections.emptyList();
	}

	public boolean tag(String bucket, String key, Map<String, String> tags) {

		List<Tag> s3Tags = tags.entrySet().stream().map(e -> Tag.builder().key(e.getKey()).value(e.getValue()).build())
				.collect(Collectors.toList());

		try {
			Tagging tagging = Tagging.builder().tagSet(s3Tags).build();
			PutObjectTaggingRequest tagReq = PutObjectTaggingRequest.builder().bucket(bucket).key(key).tagging(tagging)
					.build();
			s3Client.putObjectTagging(tagReq);
			return true;
		} catch (Exception e) {
			log.error("Error setting tag on key: {}", key, e);
		}
		return false;
	}

	public Map<String, String> tags(String bucket, String key) {
		GetObjectTaggingRequest otr = GetObjectTaggingRequest.builder().bucket(bucket).key(key).build();
		GetObjectTaggingResponse resp = s3Client.getObjectTagging(otr);
		return resp.tagSet().stream().collect(Collectors.toMap(Tag::key, Tag::value));
	}



}
