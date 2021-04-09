package uk.gov.dwp.dataworks.lambdas;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class EMRConfigurationTest {

    @BeforeAll
    public static void setup() {
        System.out.println("EMRStep unit test setup.");
    }

    @Test
    public void hasCorrectJsonStructure() throws IOException {
        EMRConfiguration test = EMRConfiguration.builder().withName("test_name").build();
        ObjectMapper mapper = new ObjectMapper();
        String event = mapper.writeValueAsString(test);
        assertEquals("{'overrides':{'Name':'test_name'}}".replaceAll("'", "\""), event);
    }
}
