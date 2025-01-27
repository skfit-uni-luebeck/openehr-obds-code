package de.uksh.medic.etl.settings;

/**
 * Settings for Kafka Consumer.
 */
public class KafkaSettings {

    private String url;
    private String username;
    private String password;
    private String group;
    private String offset;
    private String topic;
    private String clientID = "Specimen ETL 0.1";

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getClientID() {
        return clientID;
    }

    public String getGroup() {
        return group;
    }

    public String getOffset() {
        return offset;
    }

    public String getTopic() {
        return topic;
    }
}
