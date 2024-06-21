package nl.sidn.entrada2.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types.NestedField;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.io.Resource;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.load.FieldEnum;

@Data
@Slf4j
@Configuration
@ConfigurationProperties(prefix = "iceberg.catalog")
@DependsOn("s3Config")
public class IcebergCatalogConfig {

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

	@Value("classpath:avro/dns-query.avsc")
	private Resource resourceFile;

	@Bean
	Schema schema() {
		try {
			org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser()
					.parse(resourceFile.getInputStream());
			return validatedSchema(AvroSchemaUtil.toIceberg(avroSchema));
		} catch (IOException e) {
			throw new RuntimeException("Error creating Avro schema", e);
		}
	}

	@Bean
	public Catalog catalog() {
		if (StringUtils.isBlank(host)) {
			// default is aws glue catalog
			return glueCatalog();
		}

		// fallback is jdbccatalog
		return jdbcCatalog();
	}
	
	private Catalog jdbcCatalog() {
		log.info("Creating Iceberg JDBC Catalog");
		
		Map<String, String> properties = new HashMap<>();
		properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version" , "V1");
		
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
		properties.put("http-client.apache.socket-timeout-ms","10000");

		GlueCatalog catalog = new GlueCatalog();
		catalog.initialize("iceberg", properties);
		return catalog;

	}

	private Schema validatedSchema(Schema schema) {
		// check if schema fields match with the ordering used in FieldEnum
		// this may happen when the schema is changed but the enum is forgotten.

		for (NestedField field : schema.columns()) {

			if (field.fieldId() != FieldEnum.valueOf(field.name()).ordinal()) {
				throw new RuntimeException(
						"Ordering of Avro schema field \"" + field.name() + "\" not correct, expected: "
								+ field.fieldId() + " found: " + FieldEnum.valueOf(field.name()).ordinal());
			}
		}

		return schema;
	}

}
