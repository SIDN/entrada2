package nl.sidn.entrada2.service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

@Service
@Slf4j
public class S3Service {

	// prefix used for the small marker/lock objects created by claim(), kept separate from
	// the configured pcap input prefixes so it is never picked up by NewObjectChecker or
	// ExpiredObjectChecker when scanning/listing pcap objects.
	public static final String LOCK_PREFIX = ".entrada-locks/";

	private final S3Client s3Client;
	private final S3Client s3FastClient;

	public S3Service(S3Client s3Client, @Qualifier("fastClient") S3Client s3FastClient) {
		this.s3Client = s3Client;
		this.s3FastClient = s3FastClient;
	}

	public Optional<ResponseInputStream<GetObjectResponse>> read(String bucket, String key) {

		GetObjectRequest objectRequest = GetObjectRequest.builder().key(key).bucket(bucket).build();

		try {
			return Optional.of(s3Client.getObject(objectRequest));
		} catch (Exception e) {
			log.error("Error getting object {} from bucket {}, error: {}", key, bucket, e.getMessage());
			return Optional.empty();
		}
	}

	public Optional<String> readObjectAsString(String bucket, String key) {

		try (InputStream is = read(bucket, key).get()) {
			return Optional.ofNullable(StreamUtils.copyToString(is, StandardCharsets.UTF_8));

		} catch (Exception e) {
			log.error("Error object getting {} from bucket {}, error: {}", key, bucket, e.getMessage());
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

		String continuationToken = null;
		List<S3Object> s3objects = new ArrayList<S3Object>();
		
		try {
			do {
				ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
						.bucket(bucket)
						.prefix(key);
				
				if (continuationToken != null) {
					requestBuilder.continuationToken(continuationToken);
				}
				
				ListObjectsV2Response listing = s3Client.listObjectsV2(requestBuilder.build());
					
				s3objects.addAll(listing.contents());

				continuationToken = listing.isTruncated() ? listing.nextContinuationToken() : null;

				if(log.isDebugEnabled()) {
					log.debug("ls received {} objects, total so far: {}, truncated: {}", 
							listing.contents().size(), s3objects.size(), listing.isTruncated());
				}
			} while (continuationToken != null);
		
			if(log.isDebugEnabled()) {
				log.debug("ls completed, total objects: {}", s3objects.size());
			}
			return s3objects;
		} catch (Exception e) {
			log.error("ListObjectsV2Request error", e);
		}

		return Collections.emptyList();
	}

	/**
	 * Set tags for object
	 * @param bucket
	 * @param key
	 * @param tags
	 * @return
	 */
	public boolean tag(String bucket, String key, Map<String, String> tags) {

		List<Tag> s3Tags = tags.entrySet().stream().map(e -> Tag.builder().key(e.getKey()).value(e.getValue()).build())
				.collect(Collectors.toList());

		try {
			Tagging tagging = Tagging.builder().tagSet(s3Tags).build();
			PutObjectTaggingRequest tagReq = PutObjectTaggingRequest.builder().bucket(bucket).key(key).tagging(tagging)
					.build();
			s3FastClient.putObjectTagging(tagReq);
			return true;
		} catch (Exception e) {		
			if(log.isDebugEnabled()) {
				log.debug("Error setting tag on key: {}", key, e);
			}
		}
		return false;
	}

	/**
	 * Atomically claim an object for processing by creating a small marker object using a
	 * conditional PUT (If-None-Match: *). Unlike object tags (which can only be read then
	 * written, i.e. not atomically), this PUT is rejected by S3 with a 412 Precondition Failed
	 * if the marker object already exists. This guarantees that only one caller can ever
	 * successfully claim a given key, even if multiple instances (e.g. during a leader
	 * split-brain, or duplicate queue delivery) try to process the same object at the same time.
	 *
	 * @param bucket the bucket
	 * @param lockKey the key of the marker/lock object to create
	 * @return true if this call created the marker (claim acquired), false if it already
	 *         existed (already claimed by another instance) or the request failed
	 */
	public boolean claim(String bucket, String lockKey) {
		try {
			PutObjectRequest putReq = PutObjectRequest.builder()
					.bucket(bucket)
					.key(lockKey)
					.ifNoneMatch("*")
					.build();
			s3FastClient.putObject(putReq, RequestBody.empty());
			return true;
		} catch (S3Exception e) {
			if (e.statusCode() == 412) {
				// precondition failed: marker already exists, another instance claimed it first
				log.debug("Object already claimed by another instance: {}", lockKey);
			} else {
				log.error("Error claiming object: {}", lockKey, e);
			}
		} catch (Exception e) {
			log.error("Error claiming object: {}", lockKey, e);
		}

		return false;
	}

	/**
	 * Release a claim acquired via {@link #claim(String, String)}, e.g. so that a later retry
	 * for the same key can claim it again.
	 *
	 * @param bucket the bucket
	 * @param lockKey the key of the marker/lock object to remove
	 * @return true if the marker was removed (or already gone)
	 */
	public boolean releaseClaim(String bucket, String lockKey) {
		return delete(bucket, lockKey);
	}

	/**
	 * Get tags for object
	 * @param bucket
	 * @param key
	 * @param tags
	 * @return
	 */
	public boolean tags(String bucket, String key, Map<String, String> tags) {
		try {
			GetObjectTaggingRequest otr = GetObjectTaggingRequest.builder().bucket(bucket).key(key).build();
			GetObjectTaggingResponse resp = s3FastClient.getObjectTagging(otr);
			
			if(resp.tagSet().isEmpty()) {
				log.debug("No tags found for: {}", key);
				log.debug("HTTP response status for tag req: {}", resp.sdkHttpResponse().statusCode());
			}
			
			Map<String, String> tmpTags = resp.tagSet().stream().collect(Collectors.toMap(Tag::key, Tag::value));
			tags.putAll(tmpTags);
			return true;
		} catch(Exception e) {	
			if(log.isDebugEnabled()) {
				log.debug("Error getting tags for (deleted?) key: {}", key);
			}
			return false;
		} 
	}

	public boolean delete(String bucket, String key) {
		log.info("Delete object: {}", key);

		try {
			DeleteObjectRequest req = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
			s3FastClient.deleteObject(req);

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

	/**
	 * Get the size in bytes of an S3 object, or -1 if the object does not exist or an error occurs.
	 */
	public long size(String bucket, String key) {
	    try {
	        software.amazon.awssdk.services.s3.model.HeadObjectResponse response =
	            s3FastClient.headObject(HeadObjectRequest.builder()
	                .bucket(bucket)
	                .key(key)
	                .build());
	        return response.contentLength();
	    } catch (Exception e) {
	        log.error("Error getting object size for {}/{}: {}", bucket, key, e.getMessage());
	        return -1;
	    }
	}

	public boolean exists(String bucket, String key) {
	    try {
	        s3FastClient.headObject(HeadObjectRequest.builder()
	                .bucket(bucket)
	                .key(key)
	                .build());
	        return true;
	    } catch (S3Exception e) {
	        if (e.statusCode() == 404) {
	            return false;
	        }
	        throw e; // other errors bubble up
	    }
	}
	
	/**
	 * Get object metadata using HEAD request
	 * @param bucket
	 * @param key
	 * @return Optional containing S3Object with metadata, empty if not found
	 */
	public Optional<S3Object> headObject(String bucket, String key) {
	    try {
	        software.amazon.awssdk.services.s3.model.HeadObjectResponse response = 
	            s3FastClient.headObject(HeadObjectRequest.builder()
	                .bucket(bucket)
	                .key(key)
	                .build());
	        
	        // Convert HeadObjectResponse to S3Object
	        S3Object s3Object = S3Object.builder()
	            .key(key)
	            .lastModified(response.lastModified())
	            .size(response.contentLength())
	            .build();
	            
	        return Optional.of(s3Object);
	    } catch (S3Exception e) {
	        if (e.statusCode() == 404) {
	            return Optional.empty();
	        }
	        log.error("Error getting object metadata", e);
	        return Optional.empty();
	    }
	}

}
