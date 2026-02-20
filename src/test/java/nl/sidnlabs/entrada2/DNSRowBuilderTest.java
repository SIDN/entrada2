package nl.sidnlabs.entrada2;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import nl.sidn.entrada2.load.DNSRowBuilder;
import nl.sidn.entrada2.load.DnsMetricValues;
import nl.sidn.entrada2.load.FieldEnum;
import nl.sidn.entrada2.load.PacketJoiner;
import nl.sidn.entrada2.load.RowData;
import nl.sidn.entrada2.service.enrich.domain.PublicSuffixListParser;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.Packet;

@ExtendWith(MockitoExtension.class)
public class DNSRowBuilderTest extends AbstractTest {

    @InjectMocks
    private DNSRowBuilder rowBuilder;

    @Mock
    private PublicSuffixListParser domainParser;

    @BeforeEach
    void setUp() throws Exception {
        // Inject empty enrichments list (no GeoIP/ASN enrichment needed for this test)
        Field enrichmentsField = nl.sidn.entrada2.load.AbstractRowBuilder.class
                .getDeclaredField("enrichments");
        enrichmentsField.setAccessible(true);
        enrichmentsField.set(rowBuilder, Collections.emptyList());

        // Set @Value fields with their defaults
        ReflectionTestUtils.setField(rowBuilder, "privacy", false);
        ReflectionTestUtils.setField(rowBuilder, "metricsEnabled", false);
        ReflectionTestUtils.setField(rowBuilder, "rdataEnabled", false);
        ReflectionTestUtils.setField(rowBuilder, "cnameEnabled", true);
        ReflectionTestUtils.setField(rowBuilder, "rdataDnsSecEnabled", false);
        ReflectionTestUtils.setField(rowBuilder, "filteredTldsList", Collections.emptyList());

        // Trigger @PostConstruct manually to initialise filteredTlds set
        rowBuilder.init();

        // Mock the domain parser: populate the DomainResult argument and return true
        when(domainParser.parseDomainInto(any(), any())).thenAnswer(invocation -> {
            PublicSuffixListParser.DomainResult result = invocation.getArgument(1);
            result.fullDomain = "test.example.nl";
            result.registeredDomain = "example.nl";
            result.publicSuffix = "nl";
            result.subdomain = "test";
            result.labels = 3;
            result.isValid = true;
            result.tldExists = true;
            return true;
        });
    }

    /**
     * Builds the same Iceberg schema as IcebergTableConfig so GenericRecord fields
     * align with FieldEnum ordinals.
     */
    private static Schema buildSchema() {
        return new Schema(
                Types.NestedField.required(1, "dns_id", Types.IntegerType.get()),
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
                Types.NestedField.optional(32, "edns_options",
                        Types.ListType.ofOptional(33, Types.IntegerType.get())),
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
                                        Types.NestedField.required(50, "section",
                                                Types.IntegerType.get()),
                                        Types.NestedField.required(51, "type",
                                                Types.IntegerType.get()),
                                        Types.NestedField.optional(52, "data",
                                                Types.StringType.get())))),
                Types.NestedField.optional(53, "dns_cname",
                        Types.ListType.ofOptional(54, Types.StringType.get())),
                Types.NestedField.optional(55, "dns_tld", Types.StringType.get()),
                Types.NestedField.optional(56, "dns_qname_full", Types.StringType.get()));
    }
    
    @Test
    void testIpFieldResponseOnlyOkPcap() {
        PcapReader reader = createReaderFor("pcap/sidnlabs-tcp-stream-multiple-dns-message-response-only.pcap");
        PacketJoiner joiner = new PacketJoiner(10000);

        // Join all packets from the pcap into request/response pairs
        List<Packet> packets = reader.stream().collect(Collectors.toList());
        List<RowData> rowDataList = packets.stream()
                .flatMap(p -> joiner.join(p).stream())
                .collect(Collectors.toList());

        // Flush unmatched requests from the joiner cache (rcode = -1, no response)
        rowDataList.addAll(joiner.clearCache());

        assertFalse(rowDataList.isEmpty(), "Expected at least one RowData from the pcap file");

        Schema schema = buildSchema();
        RowData rowData  = rowDataList.get(0);

        //for (RowData rowData : rowDataList) {
            GenericRecord record = GenericRecord.create(schema.asStruct());
            Pair<GenericRecord, DnsMetricValues> result =
                    rowBuilder.build(rowData, "test-server", "test-location", record, null);

            assertNotSame(DNSRowBuilder.FILTERED, result,
                    "Record should not be filtered");

            GenericRecord gr = result.getKey();

            // ip_version is always available (from request or response transport)
            assertNotNull(gr.get(FieldEnum.ip_version.ordinal()),
                    "ip_version must not be null");

            // ip_dst is always available (from request or response transport)
            assertNotNull(gr.get(FieldEnum.ip_dst.ordinal()),
                    "ip_dst must not be null");
                
            assertEquals("194.0.25.24", gr.get(FieldEnum.ip_dst.ordinal()));

            // ip_src is set when privacy=false (which is the case here)
            assertNotNull(gr.get(FieldEnum.ip_src.ordinal()),
                    "ip_src must not be null (privacy is disabled)");

            assertEquals("93.190.234.217", gr.get(FieldEnum.ip_src.ordinal()));

            assertNull(gr.get(FieldEnum.ip_ttl.ordinal()),
                    "ip_ttl must not be null (privacy is disabled)");

            assertEquals(50898, gr.get(FieldEnum.prot_src_port.ordinal()));
            assertEquals(53, gr.get(FieldEnum.prot_dst_port.ordinal()));

            // ip_ttl is set from the request transport when a request is present
            if (rowData.getRequest() != null) {
                assertNotNull(gr.get(FieldEnum.ip_ttl.ordinal()),
                        "ip_ttl must not be null when a request packet is present");
            }

            assertEquals(3, gr.get(FieldEnum.dns_rcode.ordinal()));
            
        //}
    }


    /**
     * Reads the sidnlabs-test-udp-2-dns-ok.pcap file, joins packets into RowData
     * pairs, calls DNSRowBuilder.build for each pair and asserts that the ip_
     * fields populated directly from the packet (ip_ttl, ip_version, ip_src,
     * ip_dst) are not null.
     */
    @Test
    void testIpFieldsNotNullForUdpDnsOkPcap() {
        PcapReader reader = createReaderFor("pcap/sidnlabs-test-udp-2-dns-ok.pcap");
        PacketJoiner joiner = new PacketJoiner(10000);

        // Join all packets from the pcap into request/response pairs
        List<Packet> packets = reader.stream().collect(Collectors.toList());
        List<RowData> rowDataList = packets.stream()
                .flatMap(p -> joiner.join(p).stream())
                .collect(Collectors.toList());

        // Flush unmatched requests from the joiner cache (rcode = -1, no response)
        rowDataList.addAll(joiner.clearCache());

        assertFalse(rowDataList.isEmpty(), "Expected at least one RowData from the pcap file");

        Schema schema = buildSchema();
        RowData rowData  = rowDataList.get(0);

        //for (RowData rowData : rowDataList) {
            GenericRecord record = GenericRecord.create(schema.asStruct());
            Pair<GenericRecord, DnsMetricValues> result =
                    rowBuilder.build(rowData, "test-server", "test-location", record, null);

            assertNotSame(DNSRowBuilder.FILTERED, result,
                    "Record should not be filtered");

            GenericRecord gr = result.getKey();

            // ip_version is always available (from request or response transport)
            assertNotNull(gr.get(FieldEnum.ip_version.ordinal()),
                    "ip_version must not be null");

            // ip_dst is always available (from request or response transport)
            assertNotNull(gr.get(FieldEnum.ip_dst.ordinal()),
                    "ip_dst must not be null");
                
            assertEquals("194.0.28.53", gr.get(FieldEnum.ip_dst.ordinal()));

            // ip_src is set when privacy=false (which is the case here)
            assertNotNull(gr.get(FieldEnum.ip_src.ordinal()),
                    "ip_src must not be null (privacy is disabled)");

            assertEquals("74.125.73.82", gr.get(FieldEnum.ip_src.ordinal()));

            assertNotNull(gr.get(FieldEnum.ip_ttl.ordinal()),
                    "ip_ttl must not be null (privacy is disabled)");

            assertEquals(111, gr.get(FieldEnum.ip_ttl.ordinal()));

            assertEquals(64939, gr.get(FieldEnum.prot_src_port.ordinal()));
            assertEquals(53, gr.get(FieldEnum.prot_dst_port.ordinal()));

            // ip_ttl is set from the request transport when a request is present
            if (rowData.getRequest() != null) {
                assertNotNull(gr.get(FieldEnum.ip_ttl.ordinal()),
                        "ip_ttl must not be null when a request packet is present");
            }

            assertEquals(0, gr.get(FieldEnum.dns_rcode.ordinal()));
        //}
    }
}
