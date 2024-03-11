package de.uksh.medic.etl.model.mdr.centraxx;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Model for RelationConvert in Kairos CentraXX MDR.
 */
@JsonInclude(value = Include.NON_EMPTY, content = Include.NON_NULL)
public class RelationConvert {

    @JsonProperty("srcProfileCode")
    private String sourceProfileCode;
    @JsonProperty("srcProfileVersion")
    private Integer sourceProfileVersion;
    @JsonProperty("trgProfileCode")
    private String targetProfileCode;
    @JsonProperty("trgProfileVersion")
    private Integer targetProfileVersion;
    @JsonProperty("values")
    private Map<String, Object> values;
    @JsonProperty("logMessages")
    private String[] logMessages;

    public String getSourceProfileCode() {
        return sourceProfileCode;
    }

    public void setSourceProfileCode(String sourceProfileCode) {
        this.sourceProfileCode = sourceProfileCode;
    }

    public Integer getSourceProfileVersion() {
        return sourceProfileVersion;
    }

    public void setSourceProfileVersion(Integer sourceProfileVersion) {
        this.sourceProfileVersion = sourceProfileVersion;
    }

    public String getTargetProfileCode() {
        return targetProfileCode;
    }

    public void setTargetProfileCode(String targetProfileCode) {
        this.targetProfileCode = targetProfileCode;
    }

    public Integer getTargetProfileVersion() {
        return targetProfileVersion;
    }

    public void setTargetProfileVersion(Integer targetProfileVersion) {
        this.targetProfileVersion = targetProfileVersion;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public void setValues(Map<String, Object> values) {
        this.values = values;
    }

    public String[] getLogMessages() {
        return logMessages;
    }

}
