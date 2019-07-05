package uk.ac.ebi.ddi.task.ddidatasetimporter.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("importer")
public class DatasetImportTaskProperties {

    private String databaseName;

    private boolean updateStatus = true;

    private String inputDirectory;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public boolean isUpdateStatus() {
        return updateStatus;
    }

    public void setUpdateStatus(boolean updateStatus) {
        this.updateStatus = updateStatus;
    }

    public String getInputDirectory() {
        return inputDirectory;
    }

    public void setInputDirectory(String inputDirectory) {
        this.inputDirectory = inputDirectory;
    }

    @Override
    public String toString() {
        return "DatasetImportTaskProperties{" +
                "databaseName='" + databaseName + '\'' +
                ", updateStatus=" + updateStatus +
                ", inputDirectory='" + inputDirectory + '\'' +
                '}';
    }
}
