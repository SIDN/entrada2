package nl.sidn.entrada2.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.load.FieldEnum;
import software.amazon.awssdk.services.s3.S3Client;

@Slf4j
@Configuration
@DependsOn({"icebergCatalogConfig","s3Config"})
public class IcebergTableConfig {

	@Value("${iceberg.compression}")
	private String compressionAlgo;
	@Value("${iceberg.metadata.version.max:100}")
	private int metadataVersionMax;
	@Value("${iceberg.table.location}")
	private String tableLocation;
	@Value("${iceberg.table.namespace}")
	private String tableNamespace;
	@Value("${iceberg.table.name}")
	private String tableName;

	@Autowired
	private Schema schema;
	@Autowired
	private Catalog catalog;

	@Bean
	Table table() {

		Namespace namespace = Namespace.of(tableNamespace);
		TableIdentifier tableId = TableIdentifier.of(namespace, tableName);

		// only create namespace and table if this is the controller
		SupportsNamespaces catalogNs = (SupportsNamespaces) catalog;
		if (!catalogNs.namespaceExists(namespace)) {
			log.info("Create new Iceberg namespace: {}", namespace);
			catalogNs.createNamespace(namespace);
		}
		
		if (!catalog.tableExists(tableId)) {
			log.info("Create new Iceberg table: {}", tableId);

			PartitionSpec spec = PartitionSpec.builderFor(schema).day("time", "day").identity("server").build();

			Map<String, String> props = new HashMap<>();
			props.put(TableProperties.PARQUET_COMPRESSION, compressionAlgo);
			props.put(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");
			props.put(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, String.valueOf(metadataVersionMax));
			props.put(TableProperties.FORMAT_VERSION, "2");
			props.put(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + FieldEnum.dns_domainname.name(), "true");

			catalog.createTable(tableId, schema, spec, tableLocation, props);

		}

		return catalog.loadTable(tableId);
	}

}
