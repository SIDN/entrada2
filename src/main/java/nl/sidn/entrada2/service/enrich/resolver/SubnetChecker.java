/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with ENTRADA. If not, see
 * [<http://www.gnu.org/licenses/].
 *
 */
package nl.sidn.entrada2.service.enrich.resolver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SubnetChecker {

	private static final Map<String, MaskedNetwork> IPV4_CIDR_TO_MASK = new HashMap<>();
	private static final Map<String, MaskedNetwork> IPV6_CIDR_TO_MASK = new HashMap<>();

	private Cache<String, Boolean> cache = new Cache2kBuilder<String, Boolean>() {
	}.entryCapacity(10000).build();

	// Class to hold the precomputed masked network bytes and mask
	private static class MaskedNetwork {
		byte[] maskedNetwork;
		byte[] mask;

		MaskedNetwork(byte[] maskedNetwork, byte[] mask) {
			this.maskedNetwork = maskedNetwork;
			this.mask = mask;
		}
	}

	public void clear() {
		IPV4_CIDR_TO_MASK.clear();
		IPV6_CIDR_TO_MASK.clear();
	}

	public int size() {
		return IPV4_CIDR_TO_MASK.size() + IPV6_CIDR_TO_MASK.size();
	}

	public void precomputeV4Mask(String cidr, int version) {
		if (cidr == null) {
			log.error("Null value not allowed");
			return;
		}

		String[] parts = StringUtils.split(cidr, "/");

		if (parts.length != 2) {
			log.error("Invalid IP address: {}", cidr);
			return;
		}

		precomputeCidr(version == 4 ? IPV4_CIDR_TO_MASK : IPV6_CIDR_TO_MASK, cidr);
	}

	private void precomputeCidr(Map<String, MaskedNetwork> map, String cidr) {
		try {
			String[] parts = cidr.split("/");
			String networkAddress = parts[0];
			int prefixLength = Integer.parseInt(parts[1]);

			InetAddress network = InetAddress.getByName(networkAddress);
			byte[] networkBytes = network.getAddress();
			byte[] mask = createPrefixMask(prefixLength, networkBytes.length);
			
			
			byte[] masked = new byte[networkBytes.length];
			byte[] maskedNetwork = applyMask(networkBytes, mask, masked);

			map.put(cidr, new MaskedNetwork(maskedNetwork, mask));
		} catch (UnknownHostException e) {
			throw new RuntimeException("Failed to process CIDR: " + cidr, e);
		}
	}

	private byte[] createPrefixMask(int prefixLength, int byteLength) {
		byte[] mask = new byte[byteLength];
		int fullBytes = prefixLength / 8;
		int remainderBits = prefixLength % 8;

		// Set full bytes to 0xFF
		for (int i = 0; i < fullBytes; i++) {
			mask[i] = (byte) 0xFF;
		}

		// Set remaining bits
		if (remainderBits > 0) {
			mask[fullBytes] = (byte) ((0xFF << (8 - remainderBits)) & 0xFF);
		}

		return mask;
	}

	private byte[] applyMask(byte[] address, byte[] mask, byte[] masked) {
		for (int i = 0; i < address.length; i++) {
			masked[i] = (byte) (address[i] & mask[i]);
		}
		return masked;
	}

	public boolean match(String ip) {
		Boolean cached = cache.peek(ip);
		if(cached != null) {
			return cached.booleanValue();
		}
		
		InetAddress ipAddress = stringToInetAddr(ip);
		if (ipAddress == null) {
			return false;
		}

		byte[] ipBytes = ipAddress.getAddress();

		// Determine if the IP is IPv4 or IPv6
		boolean isIpv4 = ipBytes.length == 4;
		Map<String, MaskedNetwork> cidrToMaskedNetworkMap = isIpv4 ? IPV4_CIDR_TO_MASK : IPV6_CIDR_TO_MASK;

		// reuse same array for better performance
		byte[] masked = new byte[ipBytes.length];
		
		// Iterate over all precomputed CIDRs for this IP type
		for (MaskedNetwork maskedNetwork : cidrToMaskedNetworkMap.values()) {
		//	byte[] mask = maskedNetwork.mask;
			byte[] maskedIp = applyMask(ipBytes, maskedNetwork.mask, masked);

			// Compare masked IP to precomputed masked network
			if (compareBytes(maskedIp, maskedNetwork.maskedNetwork)) {
				cache.put(ip, Boolean.TRUE);
				return true; // Stop as soon as a match is found
			}
		}

		cache.put(ip, Boolean.FALSE);
		return false; // No matching CIDR found
	}

	private InetAddress stringToInetAddr(String ip) {
		try {
			return InetAddress.getByName(ip);
		} catch (UnknownHostException e) {
			return null;
		}

	}

	private boolean compareBytes(byte[] a, byte[] b) {
		if (a.length != b.length) {
			return false;
		}
		for (int i = 0; i < a.length; i++) {
			if (a[i] != b[i]) {
				return false;
			}
		}
		return true;
	}

}
