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
	@Value("${iceberg.table.location}")
	private String tableLocation;
	@Value("${iceberg.table.namespace}")
	private String tableNamespace;
	@Value("${iceberg.table.name}")
	private String tableName;
	
	private Schema schema;
	@Autowired
	private Catalog catalog;

	/**
	 * Helper class to safely build schemas with auto-incrementing field IDs.
	 * No need to manually track IDs - they're assigned sequentially starting from 1.
	 */
	private static class SchemaBuilder {
		private int nextId = 1;
		private final java.util.List<Types.NestedField> fields = new java.util.ArrayList<>();
		
		public SchemaBuilder addRequired(String name, org.apache.iceberg.types.Type type) {
			fields.add(Types.NestedField.required(nextId++, name, assignIds(type)));
			return this;
		}
		
		public SchemaBuilder addOptional(String name, org.apache.iceberg.types.Type type) {
			fields.add(Types.NestedField.optional(nextId++, name, assignIds(type)));
			return this;
		}
		
		private org.apache.iceberg.types.Type assignIds(org.apache.iceberg.types.Type type) {
			if (type instanceof Types.ListType) {
				Types.ListType listType = (Types.ListType) type;
				// Recursively assign IDs to element type (important for List<Struct>)
				return Types.ListType.ofOptional(nextId++, assignIds(listType.elementType()));
			} else if (type instanceof Types.StructType) {
				Types.StructType structType = (Types.StructType) type;
				java.util.List<Types.NestedField> newFields = new java.util.ArrayList<>();
				for (Types.NestedField field : structType.fields()) {
					if (field.isRequired()) {
						newFields.add(Types.NestedField.required(nextId++, field.name(), assignIds(field.type())));
					} else {
						newFields.add(Types.NestedField.optional(nextId++, field.name(), assignIds(field.type())));
					}
				}
				return Types.StructType.of(newFields);
			}
			return type;
		}
		
		public Schema build() {
			return new Schema(fields);
		}
		
		/**
		 * Prints all field IDs in the schema for debugging
		 */
		private static void printSchemaIds(Schema schema, String prefix) {
			for (Types.NestedField field : schema.columns()) {
				log.info("{}Field '{}' -> ID: {}", prefix, field.name(), field.fieldId());
				printFieldIds(field.type(), prefix + "  ");
			}
		}
		
		private static void printFieldIds(org.apache.iceberg.types.Type type, String prefix) {
			if (type instanceof Types.ListType) {
				Types.ListType listType = (Types.ListType) type;
				log.info("{}List element -> ID: {}", prefix, listType.elementId());
				printFieldIds(listType.elementType(), prefix + "  ");
			} else if (type instanceof Types.StructType) {
				Types.StructType structType = (Types.StructType) type;
				for (Types.NestedField field : structType.fields()) {
					log.info("{}Struct field '{}' -> ID: {}", prefix, field.name(), field.fieldId());
					printFieldIds(field.type(), prefix + "  ");
				}
			}
		}
	}

	@Bean
	Schema schema() {
		if (schema != null) {
			return schema;
		}

		// Use SchemaBuilder for safe auto-incrementing field IDs
		// any placeholder ids used below will be replaced by actual ids
		schema = new SchemaBuilder()
				.addRequired("dns_id", Types.IntegerType.get())
				.addRequired("time", Types.TimestampType.withoutZone())
				.addOptional("dns_qname", Types.StringType.get())
				.addOptional("dns_domainname", Types.StringType.get())
				.addOptional("ip_ttl", Types.IntegerType.get())
				.addOptional("ip_version", Types.IntegerType.get())
				.addOptional("prot", Types.IntegerType.get())
				.addOptional("ip_src", Types.StringType.get())
				.addOptional("prot_src_port", Types.IntegerType.get())
				.addOptional("ip_dst", Types.StringType.get())
				.addOptional("prot_dst_port", Types.IntegerType.get())
				.addOptional("dns_aa", Types.BooleanType.get())
				.addOptional("dns_tc", Types.BooleanType.get())
				.addOptional("dns_rd", Types.BooleanType.get())
				.addOptional("dns_ra", Types.BooleanType.get())
				.addOptional("dns_ad", Types.BooleanType.get())
				.addOptional("dns_cd", Types.BooleanType.get())
				.addOptional("dns_ancount", Types.IntegerType.get())
				.addOptional("dns_arcount", Types.IntegerType.get())
				.addOptional("dns_nscount", Types.IntegerType.get())
				.addOptional("dns_qdcount", Types.IntegerType.get())
				.addOptional("dns_opcode", Types.IntegerType.get())
				.addOptional("dns_rcode", Types.IntegerType.get())
				.addOptional("dns_qtype", Types.IntegerType.get())
				.addOptional("dns_qclass", Types.IntegerType.get())
				.addOptional("ip_geo_country", Types.StringType.get())
				.addOptional("ip_asn", Types.StringType.get())
				.addOptional("ip_asn_org", Types.StringType.get())
				.addOptional("edns_udp", Types.IntegerType.get())
				.addOptional("edns_version", Types.IntegerType.get())
				.addOptional("edns_do", Types.BooleanType.get())
				.addOptional("edns_options", Types.ListType.ofOptional(0, Types.IntegerType.get()))
				.addOptional("edns_ecs", Types.StringType.get())
				.addOptional("edns_ecs_ip_asn", Types.StringType.get())
				.addOptional("edns_ecs_ip_asn_org", Types.StringType.get())
				.addOptional("edns_ecs_ip_geo_country", Types.StringType.get())
				.addOptional("edns_ext_error", Types.ListType.ofOptional(0, Types.IntegerType.get()))
				.addOptional("dns_labels", Types.IntegerType.get())
				.addOptional("dns_proc_time", Types.IntegerType.get())
				.addOptional("dns_pub_resolver", Types.StringType.get())
				.addOptional("dns_req_len", Types.IntegerType.get())
				.addOptional("dns_res_len", Types.IntegerType.get())
				.addOptional("tcp_rtt", Types.IntegerType.get())
				.addRequired("server", Types.StringType.get())
				.addRequired("server_location", Types.StringType.get())
				.addOptional("dns_rdata",
						Types.ListType.ofOptional(0,
								Types.StructType.of(
										Types.NestedField.required(0, "section", Types.IntegerType.get()),
										Types.NestedField.required(0, "type", Types.IntegerType.get()),
										Types.NestedField.optional(0, "data", Types.StringType.get())
								)
						))
				.addOptional("dns_cname", Types.ListType.ofOptional(0, Types.StringType.get()))
				.addOptional("dns_tld", Types.StringType.get())
				.addOptional("dns_qname_full", Types.StringType.get())
				.addOptional("edns_server_options", Types.ListType.ofOptional(0, Types.IntegerType.get()))
				.build();
		
		log.info("Created schema with {} fields, highest ID: {}", schema.columns().size(), schema.highestFieldId());
		
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
			props.put(TableProperties.FORMAT_VERSION, "2");
			props.put(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");
			props.put(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "10");
			props.put(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + FieldEnum.dns_domainname.name(),
					"true");

			// use default warehouse s3 location
			catalog.createTable(tableId, schema, spec, props);

		}
		
		Table table = catalog.loadTable(tableId);
		// get latest schema, may already include new columns
		schema = table.schema();

		// table exists, check if need to add new columns
		// Always reload schema before adding columns to get latest field IDs
		if(schema.findField("dns_cname") == null) {
			int maxId = schema.highestFieldId();
			log.info("Updating schema to add dns_cname column (using IDs {} and {})", maxId + 1, maxId + 2);
			table.updateSchema()
				.addColumn("dns_cname", Types.ListType.ofOptional(maxId + 2, Types.StringType.get()))
				.commit();
		}
		
		if(schema.findField("dns_tld") == null) {
			schema = table.schema();
			log.info("Updating schema to add dns_tld column (ID auto-assigned)");
			table.updateSchema()
				.addColumn("dns_tld", Types.StringType.get()) 
				.commit();
		}
		
		if(schema.findField("dns_qname_full") == null) {
			schema = table.schema();
			log.info("Updating schema to add dns_qname_full column (ID auto-assigned)");
			table.updateSchema()
				.addColumn("dns_qname_full", Types.StringType.get()) 
				.commit();
		}
		
		if(schema.findField("edns_server_options") == null) {
			schema = table.schema();
			int maxId = schema.highestFieldId();
			log.info("Updating schema to add edns_server_options column (using IDs {} and {})", maxId + 1, maxId + 2);
			table.updateSchema()
				.addColumn("edns_server_options", Types.ListType.ofOptional(maxId + 2, Types.IntegerType.get()))
				.commit();
		}
		
		// Reload final schema to ensure it's current
		schema = table.schema();
		SchemaBuilder.printSchemaIds(schema, "");

		return table;
	}

}
