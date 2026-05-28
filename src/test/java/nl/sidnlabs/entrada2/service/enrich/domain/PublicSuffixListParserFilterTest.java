package nl.sidnlabs.entrada2.service.enrich.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import nl.sidn.entrada2.service.enrich.domain.PublicSuffixListParser;

class PublicSuffixListParserFilterTest {

    private PublicSuffixListParser parser;

    @BeforeEach
    void setUp() throws Exception {
        parser = new PublicSuffixListParser();

        // Configure filtering for all PSL entries ending in .nl.
        setPrivateField(parser, "hotTlds", "nl,aw");

        // Load an in-memory PSL fixture.
        invokeBuildTrie(parser, List.of(
            "nl",
            "co.nl",
            "com",
            "aw",
            "co.aw"
        ));

        parser.downloadPSLDirectAndLoad();
    }

    @Test
    void shouldUseNlAsSuffixWhenNlRulesAreFiltered() {
        PublicSuffixListParser.DomainResult result = new PublicSuffixListParser.DomainResult();

        assertTrue(parser.parseDomainInto("lab1.lab2.sidn.nl", result));
        assertEquals("sidn.nl", result.registeredDomain);
        assertEquals("nl", result.publicSuffix);
        assertEquals("lab1.lab2", result.subdomain);
        assertTrue(result.tldExists);

        assertTrue(parser.parseDomainInto("www.sidn.nl", result));
        assertEquals("sidn.nl", result.registeredDomain);
        assertEquals("nl", result.publicSuffix);
        assertEquals("www", result.subdomain);
        assertTrue(result.tldExists);

        assertTrue(parser.parseDomainInto("  www.sidn.nl  ", result));
        assertEquals("sidn.nl", result.registeredDomain);
        assertEquals("nl", result.publicSuffix);
        assertEquals("www", result.subdomain);
        assertTrue(result.tldExists);

        assertTrue(parser.parseDomainInto("www.sidn .nl", result));
        assertEquals("sidn.nl", result.registeredDomain);
        assertEquals("nl", result.publicSuffix);
        assertEquals("www", result.subdomain);
        assertTrue(result.tldExists);

        assertTrue(parser.parseDomainInto(" www. sidn .nl ", result));
        assertEquals("sidn.nl", result.registeredDomain);
        assertEquals("nl", result.publicSuffix);
        assertEquals("www", result.subdomain);
        assertTrue(result.tldExists);

        assertTrue(parser.parseDomainInto("sidn.nl", result));
        assertEquals("sidn.nl", result.registeredDomain);
        assertEquals("nl", result.publicSuffix);
        assertNull(result.subdomain);
        assertTrue(result.tldExists);

        assertTrue(parser.parseDomainInto("domain.co.nl", result));
        assertEquals("co.nl", result.registeredDomain);
        assertEquals("nl", result.publicSuffix);
        assertEquals("domain", result.subdomain);
        assertTrue(result.tldExists);

        assertTrue(parser.parseDomainInto("nl", result));
        assertNull(result.registeredDomain);
        assertEquals("nl", result.publicSuffix);
        assertNull(result.subdomain);
        assertTrue(result.tldExists);

        assertTrue(parser.parseDomainInto("domain.co.aw", result));
        assertEquals("co.aw", result.registeredDomain);
        assertEquals("aw", result.publicSuffix);
        assertEquals("domain", result.subdomain);
        assertTrue(result.tldExists);
    }

    private static void invokeBuildTrie(PublicSuffixListParser parser, List<String> rules) throws Exception {
        Method method = PublicSuffixListParser.class.getDeclaredMethod("buildTrie", List.class);
        method.setAccessible(true);
        method.invoke(parser, rules);
    }

    private static void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}