package nl.sidnlabs.entrada2;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;

import org.junit.jupiter.api.Test;

import nl.sidn.entrada2.service.enrich.resolver.SubnetChecker;

public class SubnetCheckerTest {

	private SubnetChecker checker = new SubnetChecker();

	@Test
	public void testAll() throws Exception {

		// List of IPv4 CIDRs to precompute
		String[] ipv4Cidrs = { "192.168.1.0/24", "10.0.0.0/8", "172.16.0.0/12" };

		// List of IPv6 CIDRs to precompute
		String[] ipv6Cidrs = { "2001:db8::/32", "2001:0db8:85a3::/64" };

		// Precompute masks and masked networks for IPv4 CIDRs
		for (String cidr : ipv4Cidrs) {
			checker.precomputeNetworkMask(cidr, 4);
		}

		// Precompute masks and masked networks for IPv6 CIDRs
		for (String cidr : ipv6Cidrs) {
			checker.precomputeNetworkMask(cidr, 6);
		}

		assertTrue(checker.match("192.168.1.5", InetAddress.getByName("192.168.1.5"))); // true
		assertFalse(checker.match("192.168.2.5", InetAddress.getByName("192.168.2.5"))); // false
		assertTrue(checker.match("10.1.1.1", InetAddress.getByName("10.1.1.1"))); // true
		assertTrue(checker.match("2001:db8::1", InetAddress.getByName("2001:db8::1"))); // true
		assertFalse(checker.match("2001:db9::1", InetAddress.getByName("2001:db9::1"))); // false

	}
}
