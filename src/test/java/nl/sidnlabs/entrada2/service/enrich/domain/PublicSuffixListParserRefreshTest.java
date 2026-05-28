package nl.sidnlabs.entrada2.service.enrich.domain;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import nl.sidn.entrada2.service.enrich.domain.PublicSuffixListParser;
import software.amazon.awssdk.services.s3.model.S3Object;

class PublicSuffixListParserRefreshTest {

    private PublicSuffixListParser parser;

    @BeforeEach
    void setUp() throws Exception {
        parser = new PublicSuffixListParser();
    }

    @Test
    void shouldRefreshWhenLastModifiedIsMissing() throws Exception {
        S3Object s3Object = S3Object.builder()
            .key("reference/public_suffix_list.dat")
            .build();

        assertTrue(invokeIsPslFileStale(s3Object));
    }

    @Test
    void shouldRefreshWhenObjectIsOlderThanOneDay() throws Exception {
        S3Object s3Object = S3Object.builder()
            .key("reference/public_suffix_list.dat")
            .lastModified(Instant.now().minusSeconds(25 * 60 * 60))
            .build();

        assertTrue(invokeIsPslFileStale(s3Object));
    }

    @Test
    void shouldNotRefreshWhenObjectIsNewerThanOneDay() throws Exception {
        S3Object s3Object = S3Object.builder()
            .key("reference/public_suffix_list.dat")
            .lastModified(Instant.now().minusSeconds(23 * 60 * 60))
            .build();

        assertFalse(invokeIsPslFileStale(s3Object));
    }

    private boolean invokeIsPslFileStale(S3Object s3Object) throws Exception {
        return parser.isPslFileStale(s3Object);
    }
}
