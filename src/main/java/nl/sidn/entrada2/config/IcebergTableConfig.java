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
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.load.FieldEnum;

@Slf4j
@Configuration
@DependsOn({ "icebergCatalogConfig", "s3Config" })
public class IcebergTableConfig {

	@Value("${iceberg.compression}")
	private String compressionAlgo;
	@Value("${iceberg.metadata.version.max:1}")
	private int metadataVersionMax;
	@Value("${iceberg.metadata.delete-after-commit:true}")
	private boolean metadataDeleteAfterCommit;
	@Value("${iceberg.table.location}")
	private String tableLocation;
	@Value("${iceberg.table.namespace}")
	private String tableNamespace;
	@Value("${iceberg.table.name}")
	private String tableName;
	
	private Schema schema;
	@Autowired
	private Catalog catalog;

	@Bean
	Schema schema() {
		if (schema != null) {
			return schema;
		}

		schema = new Schema(Types.NestedField.required(1, "dns_id", Types.IntegerType.get()),
				Types.NestedField.required(2, "time", Types.TimestampType.withoutZone()),
				Types.NestedField.optional(3, "dns_qname", Types.StringType.get()),
				Types.NestedField.optional(4, "dns_domainname", Types.StringType.get()),
				Types.NestedField.optional(5, "ip_ttl", Types.IntegerType.get()),
				Types.NestedField.optional(6, "ip_version", Types.IntegerType.get()),
				Types.NestedField.optional(7, "prot", Types.IntegerType.get()),
				Types.NestedField.optional(8, "ip_src", Types.StringType.get()),
				Types.NestedField.optional(9, "prot_src_port", Types.IntegerType.get()),
				Types.NestedField.optional(10, "ip_dst", Types.StringType.get()),
				Types.NestedField.optional(11, "prot_dst_port", Types.IntegerType.get()),
				Types.NestedField.optional(12, "dns_aa", Types.BooleanType.get()),
				Types.NestedField.optional(13, "dns_tc", Types.BooleanType.get()),
				Types.NestedField.optional(14, "dns_rd", Types.BooleanType.get()),
				Types.NestedField.optional(15, "dns_ra", Types.BooleanType.get()),
				Types.NestedField.optional(16, "dns_ad", Types.BooleanType.get()),
				Types.NestedField.optional(17, "dns_cd", Types.BooleanType.get()),
				Types.NestedField.optional(18, "dns_ancount", Types.IntegerType.get()),
				Types.NestedField.optional(19, "dns_arcount", Types.IntegerType.get()),
				Types.NestedField.optional(20, "dns_nscount", Types.IntegerType.get()),
				Types.NestedField.optional(21, "dns_qdcount", Types.IntegerType.get()),
				Types.NestedField.optional(22, "dns_opcode", Types.IntegerType.get()),
				Types.NestedField.optional(23, "dns_rcode", Types.IntegerType.get()),
				Types.NestedField.optional(24, "dns_qtype", Types.IntegerType.get()),
				Types.NestedField.optional(25, "dns_qclass", Types.IntegerType.get()),
				Types.NestedField.optional(26, "ip_geo_country", Types.StringType.get()),
				Types.NestedField.optional(27, "ip_asn", Types.StringType.get()),
				Types.NestedField.optional(28, "ip_asn_org", Types.StringType.get()),
				Types.NestedField.optional(29, "edns_udp", Types.IntegerType.get()),
				Types.NestedField.optional(30, "edns_version", Types.IntegerType.get()),
				Types.NestedField.optional(31, "edns_do", Types.BooleanType.get()),
				Types.NestedField.optional(32, "edns_options", Types.ListType.ofOptional(33, Types.IntegerType.get())),
				Types.NestedField.optional(34, "edns_ecs", Types.StringType.get()),
				Types.NestedField.optional(35, "edns_ecs_ip_asn", Types.StringType.get()),
				Types.NestedField.optional(36, "edns_ecs_ip_asn_org", Types.StringType.get()),
				Types.NestedField.optional(37, "edns_ecs_ip_geo_country", Types.StringType.get()),
				Types.NestedField.optional(38, "edns_ext_error",
						Types.ListType.ofOptional(39, Types.IntegerType.get())),
				Types.NestedField.optional(40, "dns_labels", Types.IntegerType.get()),
				Types.NestedField.optional(41, "dns_proc_time", Types.IntegerType.get()),
				Types.NestedField.optional(42, "dns_pub_resolver", Types.StringType.get()),
				Types.NestedField.optional(43, "dns_req_len", Types.IntegerType.get()),
				Types.NestedField.optional(44, "dns_res_len", Types.IntegerType.get()),
				Types.NestedField.optional(45, "tcp_rtt", Types.IntegerType.get()),
				Types.NestedField.required(46, "server", Types.StringType.get()),
				Types.NestedField.required(47, "server_location", Types.StringType.get()),
				Types.NestedField.optional(48, "dns_rdata",
						Types.ListType.ofOptional(49,
								Types.StructType.of(
										Types.NestedField.required(50, "section", Types.IntegerType.get()),
										Types.NestedField.required(51, "type", Types.IntegerType.get()),
										Types.NestedField.optional(52, "data", Types.StringType.get())
								)
						)
				));
				

		return schema;
	}

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

		Schema schema = schema();
		 
		if (!catalog.tableExists(tableId)) {
			log.info("Create new Iceberg table: {}", tableId);

			PartitionSpec spec = PartitionSpec.builderFor(schema)
					.day("time", "day")
					.build();

			Map<String, String> props = new HashMap<>();
			props.put(TableProperties.PARQUET_COMPRESSION, compressionAlgo);
			// auto cleanup old meta data files
			// see: https://iceberg.apache.org/docs/nightly/maintenance/#remove-old-metadata-files
			props.put(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, String.valueOf(metadataDeleteAfterCommit));
			props.put(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, String.valueOf(metadataVersionMax));
			props.put(TableProperties.FORMAT_VERSION, "2");
			props.put(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + FieldEnum.dns_domainname.name(),
					"true");

			// use default warehouse s3 location
			catalog.createTable(tableId, schema, spec, props);

		}
		
		Table table = catalog.loadTable(tableId);
		// get latest schema, may already include new columns
		schema = table.schema();
		// table exists, check if need to add new columns
		if(schema.findField("dns_cname") == null) {
			table.updateSchema()
				.addColumn("dns_cname",   Types.ListType.ofOptional(53, Types.StringType.get()))
				.commit();
		}
		
		if(schema.findField("dns_tld") == null) {
			table.updateSchema()
				.addColumn("dns_tld", Types.StringType.get())
				.commit();
		}
		
		if(schema.findField("dns_qname_full") == null) {
			table.updateSchema()
				.addColumn("dns_qname_full", Types.StringType.get())
				.commit();
		}
		
		log.info("Schema for table: {}", table.schema());

		return table;
	}

}
