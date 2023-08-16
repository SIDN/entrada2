package nl.sidn.entrada2.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

@Configuration
public class AwsConfig {
    
	@Value("${entrada.s3.access-key}")
	private String accessKey;
	
	@Value("${entrada.s3.secret-key}")
	private String secretKey;
	
	@Value("${entrada.s3.region}")
	private String region;
	
	@Value("${entrada.s3.endpoint}")
	private String endpoint;

   @Bean
    AmazonS3 s3() {
        AWSCredentials awsCredentials =
                new BasicAWSCredentials(accessKey, secretKey);
        
       
        return AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

    }
	
}
