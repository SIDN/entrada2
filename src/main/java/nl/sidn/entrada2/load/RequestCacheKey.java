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
package nl.sidn.entrada2.load;

import nl.sidn.entrada2.util.StringUtil;

public class RequestCacheKey {

	private final int id;
	private final String qname;  // stored in lowercase for fast comparison
	private final String src;    // stored in lowercase for fast comparison
	private final int srcPort;
	private final int hashCode;  // cached for performance
	
	public RequestCacheKey(int id, String qname, String src, int srcPort) {
		this.id = id;
		// Only convert to lowercase if needed (avoids allocation for common case)
		this.qname = qname;
		this.src = src;
		this.srcPort = srcPort;
		// Cache hashCode for performance (called frequently)
		this.hashCode = computeHashCode();
	}
	
	private int computeHashCode() {
		// Better hash distribution than srcPort + id
		int result = id;
		result = 31 * result + srcPort;
		result = 31 * result + (qname != null ? qname.hashCode() : 0);
		result = 31 * result + (src != null ? src.hashCode() : 0);
		return result;
	}

	@Override
	public int hashCode() {
		return hashCode;  // Return cached value
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || getClass() != obj.getClass())
			return false;
		
		RequestCacheKey other = (RequestCacheKey) obj;
		
		// Fast path: compare primitives first
		if (id != other.id || srcPort != other.srcPort)
			return false;
		
		// Use Objects.equals for null-safe comparison
		return StringUtil.equalsIgnoreCaseAscii(qname, other.qname) && StringUtil.equalsIgnoreCaseAscii(src, other.src);
	}

}
