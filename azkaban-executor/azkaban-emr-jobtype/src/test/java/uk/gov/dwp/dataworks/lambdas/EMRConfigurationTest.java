package uk.gov.dwp.dataworks.lambdas;

import com.amazonaws.util.json.Jackson;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EMRConfigurationTest {

    @BeforeAll
    public static void setup() {
        System.out.println("EMRStep unit test setup.");
    }

    @Test
    public void hasCorrectJsonStructure() {
        EMRConfiguration test = EMRConfiguration.builder().withName("test_name").build();

        String event = Jackson.toJsonString(test);
        assertEquals("{'overrides':{'Name':'test_name'}}".replaceAll("'", "\""), event);
    }
}
