package nl.sidn.entrada2.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@Configuration
@ConfigurationProperties(prefix = "iceberg.catalog")
@DependsOn("s3Config")
public class IcebergCatalogConfig {

	private String type;

	// REST catalog
	private String uri;
	private String warehouse;
	private String oauth2ServerUri;
	private String oauth2Credential;
	private String oauth2Scope;

	// jdbc config
	private String host;
	private int port;
	private String user;
	private String name;
	private String password;
	private String warehouseLocation;
	// s3 config
	@Value("${entrada.s3.endpoint}")
	private String endpoint;
	@Value("${entrada.s3.access-key}")
	private String accessKey;
	@Value("${entrada.s3.secret-key}")
	private String secretKey;

	@Bean
	public Catalog catalog() {

		return switch (StringUtils.lowerCase(type)) {
		case "rest" -> restCatalog();
		case "glue" -> glueCatalog();
		default -> jdbcCatalog();
		};

	}

	private Catalog restCatalog() {

		Map<String, String> properties = new HashMap<>();
		properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");

		properties.put(CatalogProperties.URI, uri);
		properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

		properties.put(OAuth2Properties.OAUTH2_SERVER_URI, oauth2ServerUri);
		properties.put(OAuth2Properties.CREDENTIAL, oauth2Credential);
		properties.put(OAuth2Properties.SCOPE, oauth2Scope);

		properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, secretKey);
		properties.put(S3FileIOProperties.ACCESS_KEY_ID, accessKey);

		properties.put(S3FileIOProperties.ENDPOINT, endpoint);
		properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
		properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");

		RESTCatalog catalog = new RESTCatalog();
		catalog.initialize("iceberg", properties);
		return catalog;

	}

	private Catalog jdbcCatalog() {
		log.info("Creating Iceberg JDBC Catalog");

		Map<String, String> properties = new HashMap<>();
		properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1");

		properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
		properties.put(CatalogProperties.URI, "jdbc:postgresql://" + host + ":" + port + "/" + name);
		properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", user);
		properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", password);

		properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");

		properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

		if (StringUtils.isNotBlank(endpoint)) {
			properties.put(S3FileIOProperties.ENDPOINT, endpoint);
		}

		properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, secretKey);
		properties.put(S3FileIOProperties.ACCESS_KEY_ID, accessKey);
		properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");

		JdbcCatalog catalog = new JdbcCatalog();
		catalog.initialize("iceberg", properties);
		return catalog;
	}

	private Catalog glueCatalog() {
		log.info("Creating Iceberg AWS Glue Catalog");

		Map<String, String> properties = new HashMap<>();
		properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");

		properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
		properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
		properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");

		properties.put("http-client.apache.connection-timeout-ms", "10000");
		properties.put("http-client.apache.socket-timeout-ms", "10000");

		GlueCatalog catalog = new GlueCatalog();
		catalog.initialize("iceberg", properties);
		return catalog;

	}

}
