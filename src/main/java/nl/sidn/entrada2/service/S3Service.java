package nl.sidn.entrada2.service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
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
			log.error("Error object getting {} from bucket {}", key, bucket, e);
			return Optional.empty();
		}
	}

	public Optional<String> readObectAsString(String bucket, String key) {

		try (InputStream is = read(bucket, key).get()) {
			return Optional.of(StreamUtils.copyToString(is, StandardCharsets.UTF_8));

		} catch (Exception e) {
			log.error("Error object getting {} from bucket {}", key, bucket, e);
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
		log.info("Save object: {}", key);

		try {

			log.info("Size of object: {} bytes", is.available());

			PutObjectRequest putOb = PutObjectRequest.builder().bucket(bucket).key(key).build();
			s3Client.putObject(putOb, RequestBody.fromInputStream(is, is.available()));

		} catch (Exception e) {
			log.error("Write error", e);
			return false;
		}

		return true;
	}

	public List<S3Object> ls(String bucket, String key) {

		ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucket).prefix(key).build();

		try {
			ListObjectsResponse res = s3Client.listObjects(listObjects);

			return res.contents();
			//.stream().map(o -> Pair.of(o.key(), o.lastModified())).collect(Collectors.toList());

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

	public boolean tags(String bucket, String key, Map<String, String> tags) {
		try {
			GetObjectTaggingRequest otr = GetObjectTaggingRequest.builder().bucket(bucket).key(key).build();
			GetObjectTaggingResponse resp = s3Client.getObjectTagging(otr);
			Map<String, String> tmpTags = resp.tagSet().stream().collect(Collectors.toMap(Tag::key, Tag::value));
			tags.putAll(tmpTags);
			return true;
		} catch(Exception e) {	
			log.info("Error getting tags for (deleted?) key: {}", key);
			return false;
		} 
		
		
	}

	public boolean delete(String bucket, String key) {
		log.info("Delete object: {}", key);

		try {
			DeleteObjectRequest req = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
			s3Client.deleteObject(req);

		} catch (Exception e) {
			log.error("Object delete operation failed for: " + key, e);
			return false;
		}

		return true;
	}
	
	public boolean copy(String bucket, String srcKey, String dstKey) {
		log.info("Copy object: {} to: {}", srcKey, dstKey);

		try {
			CopyObjectRequest req = CopyObjectRequest.builder().sourceBucket(bucket).destinationBucket(bucket).sourceKey(srcKey).destinationKey(dstKey).build();
			s3Client.copyObject(req);

		} catch (Exception e) {
			log.error("Object copy operation failed for: " + srcKey, e);
			return false;
		}

		return true;
	}
	
	public boolean move(String bucket, String srcKey, String dstKey) {
		log.info("Move object: {} to: {}", srcKey, dstKey);

		try {
			if(copy(bucket, srcKey, dstKey)) {
				delete(bucket, srcKey);
			}
		} catch (Exception e) {
			log.error("Object move operation failed for: " + srcKey, e);
			return false;
		}

		return true;
	}

}
