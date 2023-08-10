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
    
	@Value("${aws.accesskey}")
	private String accessKey;
	
	@Value("${aws.secretkey}")
	private String secretKey;
	
	@Value("${aws.region}")
	private String region;
	
	@Value("${aws.endpoint}")
	private String endpoint;

    @Bean
    public AmazonS3 s3() {
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