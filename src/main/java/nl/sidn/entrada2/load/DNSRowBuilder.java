package nl.sidn.entrada2.load;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.data.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.enrich.domain.PublicSuffixListParser;
import nl.sidn.entrada2.util.TimeUtil;
import nl.sidnlabs.dnslib.message.Header;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.message.Question;
import nl.sidnlabs.dnslib.message.RRset;
import nl.sidnlabs.dnslib.message.records.CNAMEResourceRecord;
import nl.sidnlabs.dnslib.message.records.ResourceRecord;
import nl.sidnlabs.dnslib.message.records.edns0.ClientSubnetOption;
import nl.sidnlabs.dnslib.message.records.edns0.EDEOption;
import nl.sidnlabs.dnslib.message.records.edns0.EDNS0Option;
import nl.sidnlabs.dnslib.message.records.edns0.OPTResourceRecord;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.pcap.packet.PacketFactory;

@Slf4j
@Component
public class DNSRowBuilder extends AbstractRowBuilder {

	public static final int RCODE_QUERY_WITHOUT_RESPONSE = -1;
	public static final int RCODE_RESPONSE_WITHOUT_QUERY = -2;
	private static final int ID_UNKNOWN = -1;
	private static final int OPCODE_UNKNOWN = -1;

	public static final Pair<GenericRecord, DnsMetricValues> FILTERED = Pair.of(null, null); 
	
	@Value("${entrada.rdata.enabled:false}")
	private boolean rdataEnabled;
	
	@Value("${entrada.cname.enabled:true}")
	private boolean cnameEnabled;
	
	@Value("${entrada.rdata.dnssec:false}")
	private boolean rdataDnsSecEnabled;
	
	@Value("#{'${entrada.filter.tlds:}'.toLowerCase().split(',')}")
	private List<String> filteredTldsList;
	
	private Set<String> filteredTlds;

	@Autowired
	private PublicSuffixListParser domainParser;
	private PublicSuffixListParser.DomainResult result = new PublicSuffixListParser.DomainResult();
	
	@PostConstruct
	public void init() {
		// Convert list to Set for O(1) lookup performance
		// Filter out empty strings from the split
		if (filteredTldsList != null && !filteredTldsList.isEmpty()) {
			filteredTlds = new HashSet<>();
			for (String tld : filteredTldsList) {
				if (tld != null && !tld.trim().isEmpty()) {
					filteredTlds.add(tld.trim().toLowerCase());
				}
			}
			if (filteredTlds.isEmpty()) {
				filteredTlds = Collections.emptySet();
			}
		} else {
			filteredTlds = Collections.emptySet();
		}
		
		if (!filteredTlds.isEmpty()) {
			log.info("TLD filtering enabled for: {}", filteredTlds);
		}
	}
	
	/**
	 * Build a GenericRecord from the given RowData, extracting relevant information from the DNS request and response messages.
	 * This method is designed to be as efficient as possible, minimizing object creation and expensive calculations
	 * When tld is in filter list, FILTERED constant is returned, which is not written to iceberg and does not cause offset to increase, but it does count as processed for metrics and retry purposes
	 * @param combo
	 * @param server
	 * @param location
	 * @param record
	 * @param recRdata
	 * @return A Pair containing the GenericRecord and DnsMetricValues, or the FILTERED constant if the TLD is in the filter list.
	 */
	public Pair<GenericRecord, DnsMetricValues> build(RowData combo, String server, String location,
			GenericRecord record, GenericRecord recRdata) {
		// try to be as efficient as possible, every object created here or expensive
		// calculation can have major impact on performance
		record.set(FieldEnum.server.ordinal(), server);

		// dns request present
		Packet reqTransport = combo.getRequest();
		Message reqMessage = combo.getRequestMessage();
		// dns response must be present
		Packet rspTransport = combo.getResponse();
		Message rspMessage = combo.getResponseMessage();
		// safe request/response
		Packet safeReqOrRespTransport = combo.getRequest() != null ? combo.getRequest() : combo.getResponse();
		Question question = null;

		// Cache timestamp to avoid multiple calls
		long tsMilli = safeReqOrRespTransport.getTsMilli();
		int prot = safeReqOrRespTransport.getProtocol();
		
		// Lazy initialization - only create metrics builder if enabled
		DnsMetricValues.DnsMetricValuesBuilder metricsBuilder = null;
		if (metricsEnabled) {
			metricsBuilder = DnsMetricValues.builder().time(tsMilli);
		}

		if (reqMessage != null && !reqMessage.getQuestions().isEmpty() ) {
			question = reqMessage.getQuestions().get(0);
		}else if (rspMessage != null && !rspMessage.getQuestions().isEmpty() ) {
			question = rspMessage.getQuestions().get(0);
		}

		// a question is not always there, e.g. UPDATE message
		if (question != null) {
			// question can be from req or resp
			// unassigned, private or unknown, get raw value
			record.set(FieldEnum.dns_qtype.ordinal(), Integer.valueOf(question.getQTypeValue()));
			
			if (metricsEnabled && metricsBuilder != null) {
				metricsBuilder.dnsQtype(question.getQType());
			}
			
			// unassigned, private or unknown, get raw value
			record.set(FieldEnum.dns_qclass.ordinal(), Integer.valueOf(question.getQClassValue()));

			boolean parserStatus = domainParser.parseDomainInto(question.getQName(), result);
			if(!parserStatus || result.publicSuffix == null) {
				// cannot get tld, save full fqdn
				record.set(FieldEnum.dns_qname_full.ordinal(), result.fullDomain);
			}else {
				// Fast TLD filtering check - O(1) Set lookup
				if (!filteredTlds.isEmpty() && result.publicSuffix != null) {
					if (filteredTlds.contains(result.publicSuffix)) {
						// TLD is in filter list - do not store this data
						return FILTERED;
					}
				}
				
				record.set(FieldEnum.dns_qname.ordinal(), result.subdomain);
				record.set(FieldEnum.dns_domainname.ordinal(), result.registeredDomain);
				record.set(FieldEnum.dns_tld.ordinal(), result.publicSuffix);
			}

			record.set(FieldEnum.dns_labels.ordinal(), result.labels);
		}

		// check to see it a response was found, if not then use -1 value for rcode
		// otherwise use the rcode returned by the server in the response.
		// no response might be caused by rate limiting
		int rcode = RCODE_QUERY_WITHOUT_RESPONSE;
		int id = ID_UNKNOWN;
		int opcode = OPCODE_UNKNOWN;

		// IP-level transport fields from request packet.
		// Guarded separately from reqMessage because a packet can exist even when
		// its DNS message is null (malformed, decoded as empty, etc.).
		if (reqTransport != null) {
			record.set(FieldEnum.ip_ttl.ordinal(), Integer.valueOf(reqTransport.getTtl()));

			if (reqTransport.getProtocol() ==  PacketFactory.PROTOCOL_TCP && reqTransport.getTcpHandshakeRTT() != -1) {
				// found tcp handshake info
				record.set(FieldEnum.tcp_rtt.ordinal(), Integer.valueOf(reqTransport.getTcpHandshakeRTT()));

				if (metricsEnabled && metricsBuilder != null) {
					metricsBuilder.tcpHandshake(reqTransport.getTcpHandshakeRTT());
				}
			}

			enrich(reqTransport.getSrc(), reqTransport.getSrcAddr(), "", record, false);

			record.set(FieldEnum.ip_dst.ordinal(), reqTransport.getDst());
			record.set(FieldEnum.prot_dst_port.ordinal(), Integer.valueOf(reqTransport.getDstPort()));
			record.set(FieldEnum.prot_src_port.ordinal(), Integer.valueOf(reqTransport.getSrcPort()));

			if (!privacy) {
				record.set(FieldEnum.ip_src.ordinal(), reqTransport.getSrc());
			}
		}else{
			// only response packet is found, use dst address for enrichment, as src address is usually
			// dns server and not the client, and enrichment is focused on client info (geo, asn)
			enrich(rspTransport.getDst(), rspTransport.getDstAddr(), "", record, false);

			record.set(FieldEnum.ip_dst.ordinal(), rspTransport.getSrc());
			// reverse src/dst port for response-only case to keep them consistent with request/response case, where dst port is usually 53 and src port is ephemeral port
			record.set(FieldEnum.prot_dst_port.ordinal(), Integer.valueOf(rspTransport.getSrcPort()));
			record.set(FieldEnum.prot_src_port.ordinal(), Integer.valueOf(rspTransport.getDstPort()));

			if (!privacy) {
				record.set(FieldEnum.ip_src.ordinal(), rspTransport.getDst());
			}
		}

		// calculate the processing time
		if (reqTransport != null && rspTransport != null) {
			int procTime = (int)(rspTransport.getTsMilli() - reqTransport.getTsMilli());
			record.set(FieldEnum.dns_proc_time.ordinal(), Integer.valueOf(procTime));
			
			if (metricsEnabled && metricsBuilder != null) {
				metricsBuilder.procTime(procTime);
			}
		}

		// fields from request DNS message
		if (reqMessage != null) {
			Header requestHeader = reqMessage.getHeader();

			id = requestHeader.getId();
			opcode = requestHeader.getRawOpcode();

			record.set(FieldEnum.dns_req_len.ordinal(),  Integer.valueOf(reqMessage.getBytes()));
			record.set(FieldEnum.dns_rd.ordinal(), Boolean.valueOf(requestHeader.isRd()));
			record.set(FieldEnum.dns_cd.ordinal(), Boolean.valueOf(requestHeader.isCd()));
			record.set(FieldEnum.dns_qdcount.ordinal(), Integer.valueOf(requestHeader.getQdCount()));

			if (metricsEnabled && metricsBuilder != null) {
				metricsBuilder.dnsQuery(true);
			}

			// EDNS0 for request
			writeRequestOptions(reqMessage, record);
		}else {
			// no request message found, may be overriden below if response message is found, but for now set rcode to indicate no request found
			rcode = RCODE_RESPONSE_WITHOUT_QUERY;
		}
		
		// fields from response
		if (rspMessage != null) {

			Header responseHeader = rspMessage.getHeader();

			if (reqMessage == null) {
				// no request message found, but response message found, we can get some values from the response, but not all, e.g. we cannot get opcode for sure, but we can get rcode
				id = responseHeader.getId();
				opcode = responseHeader.getRawOpcode();
			}

			record.set(FieldEnum.dns_res_len.ordinal(), Integer.valueOf(rspMessage.getBytes()));
			// these are the values that are retrieved from the response
			rcode = responseHeader.getRawRcode();

			record.set(FieldEnum.dns_aa.ordinal(), Boolean.valueOf(responseHeader.isAa()));
			record.set(FieldEnum.dns_tc.ordinal(), Boolean.valueOf(responseHeader.isTc()));
			record.set(FieldEnum.dns_ra.ordinal(), Boolean.valueOf(responseHeader.isRa()));
			record.set(FieldEnum.dns_ad.ordinal(), Boolean.valueOf(responseHeader.isAd()));
			record.set(FieldEnum.dns_ancount.ordinal(), Integer.valueOf(responseHeader.getAnCount()));
			record.set(FieldEnum.dns_arcount.ordinal(), Integer.valueOf(responseHeader.getArCount()));
			record.set(FieldEnum.dns_nscount.ordinal(), Integer.valueOf(responseHeader.getNsCount()));
			record.set(FieldEnum.dns_qdcount.ordinal(), Integer.valueOf(responseHeader.getQdCount()));

			// EDNS0 for response
			writeResponseOptions(rspMessage, record);

			if (metricsEnabled && metricsBuilder != null) {
				metricsBuilder.dnsResponse(true);
			}

			if(rdataEnabled) {
				// parsing and creating rdata output consumes lot of cpu/memory, it is disabled by default
				// Use reasonable initial capacity to reduce resizing
				List<GenericRecord> datas = new ArrayList<>(16);
				if(!rspMessage.getAnswer().isEmpty()) {
					rdata(rspMessage.getAnswer(), recRdata, 0, datas);
				}
				if(!rspMessage.getAuthority().isEmpty()) {
					rdata(rspMessage.getAuthority(), recRdata, 1, datas);
				}
				if(!rspMessage.getAdditional().isEmpty()) {
					rdata(rspMessage.getAdditional(), recRdata, 2, datas);
				}
				record.set(FieldEnum.dns_rdata.ordinal(), datas);
			}
			
			if(cnameEnabled && !rspMessage.getAnswer().isEmpty()) {
				// Lazy initialization - only create list if CNAMEs found
				List<String> cnames = null;
				
				for(RRset rrset: rspMessage.getAnswer()) {
					if(rrset.getType() == ResourceRecordType.CNAME) {
						for(ResourceRecord rr: rrset.getData()) {
							if(cnames == null) {
								cnames = new ArrayList<>(4);
							}
							cnames.add(((CNAMEResourceRecord)rr).getCname());
						}
					}
				}
				
				if(cnames != null) {
					record.set(FieldEnum.dns_cname.ordinal(), cnames);
				}
			}
		}
	
		record.set(FieldEnum.server_location.ordinal(), location);
		// values from request OR response now
		// if no request found in the request then use values from the response.
		record.set(FieldEnum.dns_id.ordinal(), Integer.valueOf(id));
		// Cast to int to ensure Integer boxing (getRawOpcode returns char which would box to Character)
		record.set(FieldEnum.dns_opcode.ordinal(), Integer.valueOf(opcode));
		record.set(FieldEnum.dns_rcode.ordinal(), Integer.valueOf(rcode));
		record.set(FieldEnum.time.ordinal(), TimeUtil.timestampFromMillis(tsMilli));
		record.set(FieldEnum.ip_version.ordinal(), Integer.valueOf(safeReqOrRespTransport.getIpVersion()));
		record.set(FieldEnum.prot.ordinal(), Integer.valueOf(prot));

		// create metrics
		DnsMetricValues metrics = null;
		if (metricsEnabled && metricsBuilder != null) {
			metricsBuilder.dnsRcode(rcode);
			metricsBuilder.dnsOpcode(opcode);
			metricsBuilder.ipV4(safeReqOrRespTransport.getIpVersion() == 4);
			metricsBuilder.ProtocolUdp(prot == PacketFactory.PROTOCOL_UDP);
			metricsBuilder.country((String) record.get(FieldEnum.ip_geo_country.ordinal()));
			metrics = metricsBuilder.build();
		}

		return Pair.of(record, metrics);
	}

	private void rdata(List<RRset> rrSetList, GenericRecord rec, int section, List<GenericRecord> datas){
		for(RRset rrset: rrSetList) {
		
			if(!rdataDnsSecEnabled && isDnsSecRR(rrset.getType())) {
				//skip dnssec rrs
				continue;
			}
			
			// Replace stream with simple loop for better performance
			List<ResourceRecord> records = rrset.getData();
			for (ResourceRecord rr : records) {
				// create a copy of the empty default record for each new row/record
				GenericRecord data = rec.copy();
				data.set(RdataFieldEnum.dns_rdata_section.ordinal(), section);
				data.set(RdataFieldEnum.dns_rdata_type.ordinal(), rr.getType().getValue());
				data.set(RdataFieldEnum.dns_rdata_data.ordinal(), rr.rDataToString());
				datas.add(data);
			}
		}
		
	}
	
	private boolean isDnsSecRR(ResourceRecordType rrType) {
		return rrType == ResourceRecordType.DNSKEY || rrType == ResourceRecordType.DS ||
				rrType == ResourceRecordType.RRSIG;
	}
	
	
	/**
	 * Write EDNS0 option (if any are present) to file.
	 *
	 * @param message
	 * @param builder
	 */
	private void writeResponseOptions(Message message, GenericRecord record) {
		if (message == null) {
			return;
		}

		OPTResourceRecord opt = message.getPseudo();
		if (opt != null) {
			
			if(opt.getRcode() != 0) {
				// get extended rcode
				// see: https://datatracker.ietf.org/doc/html/rfc6891
				// 12 bits code: upper 8 bits are in opt.getRcode() and lower 4 bits are in message.getHeader().getRawRcode()
				int extendedRcode = ((int) opt.getRcode() << 4) | message.getHeader().getRawRcode();
				record.set(FieldEnum.dns_rcode.ordinal(),  Integer.valueOf(extendedRcode));
			}
			
			// Lazy initialization - only create list if errors found
			List<Integer> errors = null;
			for (EDNS0Option option : opt.getOptions()) {
				if (option instanceof EDEOption) {
					if (errors == null) {
						errors = new ArrayList<>(2);
					}
					int code = ((EDEOption) option).getCode();
					errors.add(code);
				}
			}
			
			if(errors != null) {
				record.set(FieldEnum.edns_ext_error.ordinal(), errors);
			}
		}

	}

	/**
	 * Write EDNS0 option (if any are present) to file.
	 *
	 * @param message
	 * @param builder
	 */
	private void writeRequestOptions(Message message, GenericRecord record) {

		OPTResourceRecord opt = message.getPseudo();
		if (opt != null) {
			// Lazy initialization - only create list if options found
			List<Integer> ednsOptions = null;
			record.set(FieldEnum.edns_udp.ordinal(), Integer.valueOf(opt.getUdpPlayloadSize()));
			record.set(FieldEnum.edns_version.ordinal(), Integer.valueOf(opt.getVersion()));
			record.set(FieldEnum.edns_do.ordinal(), Boolean.valueOf(opt.isDnssecDo()));

			for (EDNS0Option option : opt.getOptions()) {
				if (ednsOptions == null) {
					ednsOptions = new ArrayList<>(4);
				}
				ednsOptions.add(option.getCode());

				if (option instanceof ClientSubnetOption) {
					ClientSubnetOption scOption = (ClientSubnetOption) option;
					// get client country and asn

					if (scOption.getAddress() != null) {
						enrich(scOption.getAddress(), scOption.getInetAddress(), "edns_ecs_", record, true);
					}

					if (!privacy) {
						record.set(FieldEnum.edns_ecs.ordinal(),
								scOption.getAddress() + "/" + scOption.getSourcenetmask());
					}
				}
			}

			if(ednsOptions != null) {
				record.set(FieldEnum.edns_options.ordinal(), ednsOptions);
			}
		}

	}

}
