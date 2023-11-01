package nl.sidn.entrada2.load;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.data.GenericRecord;
import org.springframework.stereotype.Component;

import com.google.common.base.CharMatcher;

import nl.sidn.entrada2.util.TimeUtil;
import nl.sidnlabs.dnslib.message.Header;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.message.Question;
import nl.sidnlabs.dnslib.message.records.edns0.ClientSubnetOption;
import nl.sidnlabs.dnslib.message.records.edns0.EDEOption;
import nl.sidnlabs.dnslib.message.records.edns0.EDNS0Option;
import nl.sidnlabs.dnslib.message.records.edns0.OPTResourceRecord;
import nl.sidnlabs.dnslib.util.NameUtil;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.pcap.packet.PacketFactory;

@Component
public class DNSRowBuilder extends AbstractRowBuilder {

	private static final int RCODE_QUERY_WITHOUT_RESPONSE = -1;
	private static final int ID_UNKNOWN = -1;
	private static final int OPCODE_UNKNOWN = -1;

	// @Override
	public Pair<GenericRecord, DnsMetricValues> build(RowData combo, String server, String location,
			GenericRecord record) {
		// try to be as efficient as possible, every object created here or expensive
		// calculation can
		// have major impact on performance

		record.set(FieldEnum.server.ordinal(), server);

		// dns request present
		Packet reqTransport = combo.getRequest();
		Message reqMessage = combo.getRequestMessage();
		// dns response must be present
		Packet rspTransport = combo.getResponse();
		Message rspMessage = combo.getResponseMessage();
		// safe request/response
		Packet transport = combo.getRequest() != null ? combo.getRequest() : combo.getResponse();
		Question question = null;

		DnsMetricValues.DnsMetricValuesBuilder metricsBuilder = DnsMetricValues.builder().time(transport.getTsMilli());

		// check to see it a response was found, if not then use -1 value for rcode
		// otherwise use the rcode returned by the server in the response.
		// no response might be caused by rate limiting
		int rcode = RCODE_QUERY_WITHOUT_RESPONSE;
		int id = ID_UNKNOWN;
		int opcode = OPCODE_UNKNOWN;

		// fields from request
		if (reqMessage != null) {
			Header requestHeader = reqMessage.getHeader();

			id = requestHeader.getId();
			opcode = requestHeader.getRawOpcode();

			if (!reqMessage.getQuestions().isEmpty()) {
				// request specific

				question = reqMessage.getQuestions().get(0);

				record.set(FieldEnum.req_len.ordinal(), Integer.valueOf(reqMessage.getBytes()));

				// only add IP DF flag for server response packet
				record.set(FieldEnum.ip_req_df.ordinal(), Boolean.valueOf(reqTransport.isDoNotFragment()));

				if (reqTransport.getTcpHandshakeRTT() != -1) {
					// found tcp handshake info
					record.set(FieldEnum.tcp_hs_rtt.ordinal(), Integer.valueOf(reqTransport.getTcpHandshakeRTT()));

					if (metricsEnabled) {
						metricsBuilder.tcpHandshake(reqTransport.getTcpHandshakeRTT());
					}
				}
			}

			record.set(FieldEnum.ttl.ordinal(), Integer.valueOf(reqTransport.getTtl()));
			record.set(FieldEnum.rd.ordinal(), Boolean.valueOf(requestHeader.isRd()));
			record.set(FieldEnum.z.ordinal(), Boolean.valueOf(requestHeader.isZ()));
			record.set(FieldEnum.cd.ordinal(), Boolean.valueOf(requestHeader.isCd()));
			record.set(FieldEnum.qdcount.ordinal(), Integer.valueOf(requestHeader.getQdCount()));
			record.set(FieldEnum.query_f_tc.ordinal(), Boolean.valueOf(requestHeader.isTc()));
			record.set(FieldEnum.query_f_ra.ordinal(), Boolean.valueOf(requestHeader.isRa()));
			record.set(FieldEnum.query_f_ad.ordinal(), Boolean.valueOf(requestHeader.isAd()));

			// ip fragments in the request
			if (reqTransport.isFragmented()) {
				record.set(FieldEnum.ip_frag.ordinal(), Integer.valueOf(reqTransport.getReassembledFragments()));
			}

			if (metricsEnabled) {
				metricsBuilder.dnsQuery(true);
			}

			// EDNS0 for request
			writeRequestOptions(reqMessage, record);
		}

		// fields from response
		if (rspMessage != null) {
			Header responseHeader = rspMessage.getHeader();

			id = responseHeader.getId();
			opcode = responseHeader.getRawOpcode();

			if (!rspMessage.getQuestions().isEmpty()) {
				// response specific

				if (question == null) {
					question = rspMessage.getQuestions().get(0);
				}

				record.set(FieldEnum.res_len.ordinal(), Integer.valueOf(rspMessage.getBytes()));

				// only add IP DF flag for server response packet
				record.set(FieldEnum.ip_res_df.ordinal(), Boolean.valueOf(rspTransport.isDoNotFragment()));

				// these are the values that are retrieved from the response
				rcode = responseHeader.getRawRcode();

				record.set(FieldEnum.aa.ordinal(), Boolean.valueOf(responseHeader.isAa()));
				record.set(FieldEnum.tc.ordinal(), Boolean.valueOf(responseHeader.isTc()));
				record.set(FieldEnum.ra.ordinal(), Boolean.valueOf(responseHeader.isRa()));
				record.set(FieldEnum.ad.ordinal(), Boolean.valueOf(responseHeader.isAd()));
				record.set(FieldEnum.ancount.ordinal(), Integer.valueOf(responseHeader.getAnCount()));
				record.set(FieldEnum.arcount.ordinal(), Integer.valueOf(responseHeader.getArCount()));
				record.set(FieldEnum.nscount.ordinal(), Integer.valueOf(responseHeader.getNsCount()));
				record.set(FieldEnum.qdcount.ordinal(), Integer.valueOf(responseHeader.getQdCount()));

				// ip fragments in the response
				if (rspTransport.isFragmented()) {
					int frags = rspTransport.getReassembledFragments();
					record.set(FieldEnum.ip_resp_frag.ordinal(), Integer.valueOf(frags));
				}

				// EDNS0 for response
				writeResponseOptions(rspMessage, record);

				if (metricsEnabled) {
					metricsBuilder.dnsResponse(true);

				}
			}
		}

		String qname = null;
		String domainname = null;
		int labels = -1;

		// a question is not always there, e.g. UPDATE message
		if (question != null) {
			// question can be from req or resp
			// unassigned, private or unknown, get raw value
			record.set(FieldEnum.qtype.ordinal(), Integer.valueOf(question.getQTypeValue()));
			
			if (metricsEnabled) {
				metricsBuilder.dnsQtype(question.getQTypeValue());
			}
			
			// unassigned, private or unknown, get raw value
			record.set(FieldEnum.qclass.ordinal(), Integer.valueOf(question.getQClassValue()));

			// remove non asccii chars
			qname = CharMatcher.ascii().negate().or(CharMatcher.invisible()).removeFrom(question.getQName());
			
			if (NameUtil.isValid(qname)) {
				// only create domainname when qname contains only valid ascii chars
				domainname = domainCache.peek(qname);
				labels = NameUtil.labels(qname);

				if (domainname == null) {
					domainname = NameUtil.domainname(qname);
					if (domainname != null) {
						domainCache.put(qname, domainname);
					}
				}
			}

			
		}

		// only save the part of the qname thats not part the domainname, this saves 
		// s3 storage and athena io/read costs


		// if no domainname found in qname, then save full qname
		if (domainname == null) {
			record.set(FieldEnum.qname.ordinal(), qname);
		} else {
			record.set(FieldEnum.domainname.ordinal(), domainname);
			// check if qname and domain are same length, if same then qname is not saved.
			if (domainname.length() != qname.length() - 1) {
				// get substring, include 2 dots. 1 at the end and 1 between qname part and domain part
				record.set(FieldEnum.qname.ordinal(), qname.substring(0, qname.length() - (domainname.length() + 2)));
			}
		}

		if (labels > -1) {
			record.set(FieldEnum.labels.ordinal(), Integer.valueOf(labels));
		}

		record.set(FieldEnum.server_location.ordinal(), location);

		// add file name, makes it easier to find the original input pcap
		// in case of of debugging.
		record.set(FieldEnum.pcap_file.ordinal(), transport.getFilename());

		// values from request OR response now
		// if no request found in the request then use values from the response.
		record.set(FieldEnum.id.ordinal(), Integer.valueOf(id));
		record.set(FieldEnum.opcode.ordinal(), Integer.valueOf(opcode));
		record.set(FieldEnum.rcode.ordinal(), Integer.valueOf(rcode));
		record.set(FieldEnum.time.ordinal(), TimeUtil.timestampFromMillis(transport.getTsMilli()));
		record.set(FieldEnum.ipv.ordinal(), Integer.valueOf(transport.getIpVersion()));

		int prot = transport.getProtocol();
		record.set(FieldEnum.prot.ordinal(), Integer.valueOf(prot));

		// get ip src/dst from either request of response
		if (reqTransport != null) {
			enrich(reqTransport.getSrc(), reqTransport.getSrcAddr(), "", record, false);

			record.set(FieldEnum.dst.ordinal(), reqTransport.getDst());
			record.set(FieldEnum.dstp.ordinal(), Integer.valueOf(reqTransport.getDstPort()));
			record.set(FieldEnum.srcp.ordinal(), Integer.valueOf(reqTransport.getSrcPort()));

			if (!privacy) {
				record.set(FieldEnum.src.ordinal(), reqTransport.getSrc());
			}
		} else {

			// only response packet is found
			enrich(rspTransport.getDst(), rspTransport.getDstAddr(), "", record, false);

			record.set(FieldEnum.dst.ordinal(), rspTransport.getDst());
			record.set(FieldEnum.dstp.ordinal(), Integer.valueOf(rspTransport.getDstPort()));
			record.set(FieldEnum.srcp.ordinal(), Integer.valueOf(rspTransport.getSrcPort()));

			if (!privacy) {
				record.set(FieldEnum.src.ordinal(), rspTransport.getSrc());
			}
		}

		// calculate the processing time
		if (reqTransport != null && rspTransport != null) {
			record.set(FieldEnum.proc_time.ordinal(),
					Integer.valueOf((int) (rspTransport.getTsMilli() - reqTransport.getTsMilli())));
		}

		// create metrics
		if (metricsEnabled) {
			metricsBuilder.dnsRcode(rcode);
			metricsBuilder.dnsOpcode(opcode);
			metricsBuilder.ipV4(transport.getIpVersion() == 4);
			metricsBuilder.ProtocolUdp(prot == PacketFactory.PROTOCOL_UDP);
			metricsBuilder.country((String) record.get(FieldEnum.country.ordinal()));
		}

		return Pair.of(record, metricsBuilder.build());
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
			for (EDNS0Option option : opt.getOptions()) {
				if (option instanceof EDEOption) {
					int code = ((EDEOption) option).getCode();
					record.set(FieldEnum.edns_ext_error.ordinal(), code);
					// this is the only server edns data we support, stop processing other options
					break;
				}
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
			List<Integer> ednsOptions = new ArrayList<>();
			record.set(FieldEnum.edns_udp.ordinal(), Integer.valueOf(opt.getUdpPlayloadSize()));
			record.set(FieldEnum.edns_version.ordinal(), Integer.valueOf(opt.getVersion()));
			record.set(FieldEnum.edns_do.ordinal(), Boolean.valueOf(opt.isDnssecDo()));

			for (EDNS0Option option : opt.getOptions()) {

				ednsOptions.add(option.getCode());
//        if (option instanceof PingOption) {
//          record.set(FieldEnum.edns_ping.ordinal(), Boolean.TRUE);
//        } else if (option instanceof DNSSECOption) {
//          if (option.getCode() == DNSSECOption.OPTION_CODE_DAU) {
//            record.set(FieldEnum.edns_dnssec_dau.ordinal(), ((DNSSECOption) option).export());
//          } else if (option.getCode() == DNSSECOption.OPTION_CODE_DHU) {
//            record.set(FieldEnum.edns_dnssec_dhu.ordinal(), ((DNSSECOption) option).export());
//          } else { // N3U
//            record.set(FieldEnum.edns_dnssec_n3u.ordinal(), ((DNSSECOption) option).export());
//          }
//        } else
				if (option instanceof ClientSubnetOption) {
					ClientSubnetOption scOption = (ClientSubnetOption) option;
					// get client country and asn

					if (scOption.getAddress() != null) {
						enrich(scOption.getAddress(), scOption.getInetAddress(), "edns_client_subnet_", record, true);
					}

					if (!privacy) {
						record.set(FieldEnum.edns_client_subnet.ordinal(),
								scOption.getAddress() + "/" + scOption.getSourcenetmask());
					}

//        } else if (option instanceof PaddingOption) {
//          record
//              .set(FieldEnum.edns_padding.ordinal(),
//                  Integer.valueOf(((PaddingOption) option).getLength()));
//        } else if (option instanceof KeyTagOption) {
//          KeyTagOption kto = (KeyTagOption) option;
//          record
//              .set(FieldEnum.edns_keytag_count.ordinal(), Integer.valueOf(kto.getKeytags().size()));
//
//          if (!kto.getKeytags().isEmpty()) {
//            record
//                .set(FieldEnum.edns_keytag_list.ordinal(),
//                    kto
//                        .getKeytags()
//                        .stream()
//                        .map(Object::toString)
//                        .collect(Collectors.joining(",")));
//          }
//        }
//        else {
//          // other
//          if (otherEdnsOptions == null) {
//            otherEdnsOptions = new ArrayList<>();
//          }
//          otherEdnsOptions.add(Integer.valueOf(option.getCode()));
//        }
				}
			}
//      if (!ednsOptions.isEmpty()) {
//       // if (otherEdnsOptions.size() == 1) {
//          record.set(FieldEnum.edns_options.ordinal(), ednsOptions);
////        } else {
////          StringBuilder sb = new StringBuilder();
////          for (Integer option : otherEdnsOptions) {
////            sb.append(option.toString());
////          }
////
////          record.set(FieldEnum.edns_other.ordinal(), sb.toString());
////        }
//
//      }

			record.set(FieldEnum.edns_options.ordinal(), ednsOptions);
		}

	}

}
