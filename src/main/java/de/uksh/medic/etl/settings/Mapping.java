package de.uksh.medic.etl.settings;

/**
 * Defines mappings between source and target profiles used for MDR mapping.
 */
public class Mapping {

    private String source;
    private Integer sourceVersion;
    private String target;
    private Integer targetVersion;
    private boolean split;
    private String templateId;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Integer getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(Integer sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public Integer getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(Integer targetVersion) {
        this.targetVersion = targetVersion;
    }

    public boolean getSplit() {
        return split;
    }

    public void setSplit(boolean split) {
        this.split = split;
    }

    public String getTemplateId() {
        return templateId;
    }

    public void setTemplateId(String templateId) {
        this.templateId = templateId;
    }

}
