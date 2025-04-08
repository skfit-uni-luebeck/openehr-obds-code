package de.uksh.medic.etl.settings;

/**
 * Settings for Kafka Consumer.
 */
public class KafkaSettings {
    private static final int DEFAULT_POLL_DURATION = 1000;

    private String url;
    private String group;
    private String offset;
    private String readTopic;
    private String errorTopic;
    private int pollDuration;
    private int pollRecords;
    private String clientID;

    public String getUrl() {
        return url;
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

    public String getReadTopic() {
        return readTopic;
    }

    public String getErrorTopic() {
        return errorTopic;
    }

    public int getPollDuration() {
        return pollDuration > 0 ? pollDuration : DEFAULT_POLL_DURATION;
    }

    public String getPollRecords() {
        return pollRecords > 0 ? String.valueOf(pollRecords) : "5";
    }
}
