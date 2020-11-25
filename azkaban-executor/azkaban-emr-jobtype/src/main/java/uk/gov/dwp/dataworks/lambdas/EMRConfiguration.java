package uk.gov.dwp.dataworks.lambdas;

import org.codehaus.jackson.annotate.JsonProperty;

public class EMRConfiguration {
    private Overrides overrides;

    public EMRConfiguration(String name){
        this.overrides = new Overrides(name);
    }

    public static EMRConfigurationBuilder builder() {
        return new EMRConfigurationBuilder();
    }

    public Overrides getOverrides() {
        return overrides;
    }

    public static class EMRConfigurationBuilder {
        private String name;

        public EMRConfigurationBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public EMRConfiguration build() {
            return new EMRConfiguration(this.name);
        }
    }

    public static class Overrides {

        private String name;

        public Overrides(String name) {
            this.name = name;
        }

        @JsonProperty("Name")
        public String getName() {
            return this.name;
        }
    }
}
