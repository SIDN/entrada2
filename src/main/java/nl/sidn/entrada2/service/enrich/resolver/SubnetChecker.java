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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SubnetChecker {

	// Lists give O(1) indexed iteration without HashMap bucket overhead or iterator allocation
	private final List<MaskedNetwork> IPV4_NETWORKS = new ArrayList<>();
	private final List<MaskedNetwork> IPV6_NETWORKS = new ArrayList<>();

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
		IPV4_NETWORKS.clear();
		IPV6_NETWORKS.clear();
	}

	public int size() {
		return IPV4_NETWORKS.size() + IPV6_NETWORKS.size();
	}

	public void precomputeNetworkMask(String cidr, int version) {
		if (cidr == null) {
			log.error("Null value not allowed");
			return;
		}

		String[] parts = StringUtils.split(cidr, "/");

		if (parts.length != 2) {
			log.error("Invalid IP address: {}", cidr);
			return;
		}

		precomputeCidr(version == 4 ? IPV4_NETWORKS : IPV6_NETWORKS, cidr);
	}

	private void precomputeCidr(List<MaskedNetwork> list, String cidr) {
		try {
			String[] parts = cidr.split("/");
			String networkAddress = parts[0];
			int prefixLength = Integer.parseInt(parts[1]);

			InetAddress network = InetAddress.getByName(networkAddress);
			byte[] networkBytes = network.getAddress();
			byte[] mask = createPrefixMask(prefixLength, networkBytes.length);
			byte[] maskedNetwork = new byte[networkBytes.length];
			applyMask(networkBytes, mask, maskedNetwork);

			list.add(new MaskedNetwork(maskedNetwork, mask));
		} catch (UnknownHostException e) {
			log.error("Failed to process CIDR: " + cidr, e);
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

	public boolean match(String ip, InetAddress inetAddress) {
		Boolean cached = cache.peek(ip);
		if(cached != null) {
			return cached.booleanValue();
		}
		
		if (inetAddress == null) {
			return false;
		}

		byte[] ipBytes = inetAddress.getAddress();

		// Determine if the IP is IPv4 or IPv6
		List<MaskedNetwork> networks = ipBytes.length == 4 ? IPV4_NETWORKS : IPV6_NETWORKS;
		int networkCount = networks.size();

		// Fused mask-and-compare: avoids intermediate masked[] write and a second loop.
		// Index-based iteration avoids iterator allocation on every call.
		for (int n = 0; n < networkCount; n++) {
			MaskedNetwork mn = networks.get(n);
			byte[] mask = mn.mask;
			byte[] net  = mn.maskedNetwork;
			boolean matched = true;
			for (int j = 0; j < ipBytes.length; j++) {
				if ((ipBytes[j] & mask[j]) != net[j]) {
					matched = false;
					break;
				}
			}
			if (matched) {
				cache.put(ip, Boolean.TRUE);
				return true;
			}
		}

		cache.put(ip, Boolean.FALSE);
		return false; // No matching CIDR found
	}

}
