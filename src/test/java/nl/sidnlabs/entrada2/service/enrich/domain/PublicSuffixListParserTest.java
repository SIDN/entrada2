package nl.sidnlabs.entrada2.service.enrich.domain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import nl.sidn.entrada2.service.enrich.domain.PublicSuffixListParser;
import nl.sidnlabs.dnslib.util.NameUtil;

public class PublicSuffixListParserTest {

    private static PublicSuffixListParser validator;
    private static PublicSuffixListParser.DomainResult result;

    @BeforeAll
    @SuppressWarnings("deprecation")
    public static void setup() throws IOException {
        validator = new PublicSuffixListParser();
        result = new PublicSuffixListParser.DomainResult();
        // Load PSL data - using deprecated method for backward compatibility in tests
        validator.downloadPSLDirectAndLoad();
    }

    private String domainname(String fqdn) {
        validator.parseDomainInto(fqdn, result);
        return result.registeredDomain;
    }
    
    private String qname(String fqdn) {
        validator.parseDomainInto(fqdn, result);
        return result.subdomain;
    }
    
    private String tld(String fqdn) {
        validator.parseDomainInto(fqdn, result);
        return result.publicSuffix;
    }

    private int labels(String fqdn) {
        validator.parseDomainInto(fqdn, result);
        return result.labels;
    }

    private boolean tldExists(String fqdn) {
        validator.parseDomainInto(fqdn, result);
        return result.tldExists;
    }
    
	// public static int labels(String qname) {

	// 	if (qname == null || qname.length() == 0 || (qname.length() == 1 && qname.charAt(0) == '.')) {
	// 		return 0;
	// 	}

	// 	if (qname.charAt(0) == '.') {
	// 		// ignoring starting .
	// 		return StringUtils.countMatches(qname, '.') - 1;
	// 	}

	// 	return StringUtils.countMatches(qname, '.');
	// }

    @Test
    public void testIsValidOk() {
        // simple sanity: we could consider valid if domainname() returns non-null
        assertNotNull(domainname("sidn.nl."));
        assertEquals("sidn.nl", domainname("%%%.sidn.nl."));
    }
    
    @Test
    public void testNonAsciiOk() {
        // simple sanity: we could consider valid if domainname() returns non-null
    	 assertEquals("sidn.nl",domainname("^B�^Er@ve.sidn.nl."));
    }

    @Test
    public void testExtractDomainLevelOk() {
    	assertEquals("*.nl", domainname("*.nl."));
    	assertEquals("nl", tld("*.nl."));
        assertNull(domainname("nl"));
        
        assertEquals("backmitra.nl", domainname("*.backmitra.nl."));
    	assertEquals("nl", tld("*.backmitra.nl."));
    	assertEquals(3, labels("*.backmitra.nl."));
        
    	assertEquals(true, NameUtil.isValid("*.backmitra.nl."));

        assertEquals("sidn.nl", domainname("sidn.nl."));
        assertEquals(2, labels("sidn.nl."));

        assertEquals("sidn.nl", domainname("www.sidn.nl."));
        assertEquals(3, labels("www.sidn.nl."));

        assertEquals("sidn.nl", domainname("test.www.sidn.nl."));
        assertEquals(4, labels("test.www.sidn.nl."));

        assertEquals("blogspot.co.uk", domainname("test.blogspot.co.uk."));
        assertEquals(4, labels("test.blogspot.co.uk."));

        assertEquals("nzrs.co.nz", domainname("www.nzrs.co.nz."));
        assertEquals(4, labels("www.nzrs.co.nz."));

        assertEquals("bpw.net.nz", domainname("_gc._tcp.bpw.net.nz."));
        assertEquals(5, labels("_gc._tcp.bpw.net.nz."));

        assertEquals("bpw.net.nz", domainname("_gc._tcp.default-first-site-name._sites.bpw.net.nz."));
        assertEquals(7, labels("_gc._tcp.default-first-site-name._sites.bpw.net.nz."));
    }

    @Test
    public void testEmailAddress2ndLevelOk() {
        assertEquals("test@example.com", domainname("email.test@example.com."));
    }

    @Test
    public void testDomainWith2ndLevelAndTldSuffixOk() {
        String domain = domainname("name.example.co.uk.");
        assertNotNull(domain);
        assertEquals("example.co.uk", domain);
        assertEquals(4, labels("name.example.co.uk."));
    }

    @Test
    public void testInvalidQnameOk() {
    	assertEquals("palazzodesign.co.nz", domainname("#192.168.51.52.palazzodesign.co.nz."));
        assertEquals("???192.168.51.52", qname("➊➋➌192.168.51.52.palazzodesign.co.nz."));
        assertEquals(7, labels("➊➋➌192.168.51.52.palazzodesign.co.nz."));
        
        assertEquals("sidn.nl", domainname("-sub1.sidn.nl."));
        assertEquals("nl", tld("-sub1.sidn.nl."));
        assertEquals("-sub1", qname("-sub1.sidn.nl."));
        assertEquals(3, labels("-sub1.sidn.nl."));

        assertEquals("sidn.nl", domainname("test .sidn.nl."));
        assertEquals(3, labels("test .sidn.nl."));

        assertEquals("anzrad.co.nz", domainname("_.anzrad.co.nz."));
        assertEquals(4, labels("_.anzrad.co.nz."));
        
        assertEquals("_anzrad.co.nz", domainname("_anzrad.co.nz."));
        assertEquals(4, labels("_.anzrad.co.nz."));

        assertEquals("0x10 0x190x1xp0x190x1ds.ac.nz",domainname("r._dns-sd._udp.0x10 0x190x1xp0x190x1ds.ac.nz."));
        assertEquals(6, labels("r._dns-sd._udp.0x10 0x190x1xp0x190x1ds.ac.nz."));

        assertEquals("aklc-guest.govt.nz", domainname("https.aklc-guest.govt.nz."));
        assertEquals(4, labels("https.aklc-guest.govt.nz."));

        assertEquals("yyyy@lincolnuni.ac.nz", domainname("xxxx.yyyy@lincolnuni.ac.nz."));
        assertEquals(4, labels("xxxx.yyyy@lincolnuni.ac.nz."));

        assertEquals("0x1bequ??t?enable.net.nz",domainname("_ldap._tcp.dc._msdcs.workgroup.0x1bequ??t?enable.net.nz."));
        assertEquals(8, labels("_ldap._tcp.dc._msdcs.workgroup.0x1bequ??t?enable.net.nz."));

        
    }
    
    @Test
    public void testNlOk() {
    	assertEquals("sidn.nl", domainname("*.sidn.nl."));
    	assertEquals("*", qname("*.sidn.nl."));
        assertEquals("nl", tld("*.sidn.nl."));
        assertEquals(3, labels("*.sidn.nl."));
        
        assertEquals("sidn.nl", domainname("sidn.nl."));
        assertEquals(2, labels("sidn.nl."));
        
        assertEquals("sidn.nl", domainname("www.sidn.nl."));
        assertEquals("www", qname("www.sidn.nl."));
        assertEquals(3, labels("wwww.sidn.nl."));
        
        
        assertNull(domainname("nl."));
        assertNull(qname("nl."));
        assertEquals("nl", tld("nl."));
        assertEquals(1, labels("nl."));

        assertNull(domainname(".nl."));
        assertNull(qname(".nl."));
        assertEquals("nl", tld(".nl."));
        assertEquals(1, labels(".nl."));
    }
    
    @Test
    public void testNlInvalidOk() {
        assertNull(domainname(".nl."));
        assertNull(qname(".nl."));
        assertEquals("nl", tld(".nl."));
        assertEquals(1, labels(".nl."));
    }

    @Test
    public void testPublicSuffixOk() {
        assertNull(domainname("nl."));
        assertEquals(1, labels("nl."));

        assertNull(domainname(".nl."));
        assertEquals(1, labels(".nl."));

        assertNull(domainname("co.nz."));
        assertEquals(2, labels("co.nz."));

        assertEquals("blogspot.co.nz", domainname("ruanca.blogspot.co.nz."));
        assertEquals(4, labels("ruanca.blogspot.co.nz."));
    }

    @Test
    public void testEmptyQnameOk() {
        assertNull(domainname("."));
        assertEquals(0, labels("."));

        assertNull(domainname(null));
        assertEquals(0, labels(null));

        assertNull(domainname(""));
        assertEquals(0, labels(""));
    }
    
    @Test
    public void testInvalidTldOk() {
    	assertEquals("xxx.doesnotexists", domainname("xxx.doesnotexists."));
        assertEquals(false, tldExists("xxx.doesnotexists."));
    	assertNull(qname("xxx.doesnotexists."));
    }

    @Test
    public void testSpecialTldOk() {
    	assertEquals("hostname.bind", domainname("hostname.bind."));
        assertEquals("bind", tld("hostname.bind."));
        assertEquals("bind", tld(".hostname.bind."));
        assertEquals("bind", tld("bind."));
        assertEquals(false, tldExists("hostname.bind."));
        assertEquals("version.bind", domainname("version.bind."));
        assertEquals("bind", tld("version.bind."));
        assertEquals(false, tldExists("version.bind."));
    }


    @Test
    public void testPubSuffixOk() throws Exception{
    	
    	String domain = "www.sidn.nl.";
    	validator.parseDomainInto(domain, result);
    	
    	//DomainInfo di = validator.getDomainInfo("www.sidn.nl.");
    	assertEquals("sidn.nl", result.registeredDomain);
    	assertEquals("nl", result.publicSuffix);
    	assertEquals("www", result.subdomain);
    	
    	domain = "www.test.sidn.co.uk.";
    	validator.parseDomainInto(domain, result);
    	
    	assertEquals("sidn.co.uk", result.registeredDomain);
    	assertEquals("co.uk", result.publicSuffix);
    	assertEquals("www.test", result.subdomain);

    }
    
    
}
