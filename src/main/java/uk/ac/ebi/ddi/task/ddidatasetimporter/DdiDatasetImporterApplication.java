package uk.ac.ebi.ddi.task.ddidatasetimporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import uk.ac.ebi.ddi.ddifileservice.DdiFileServiceApplication;
import uk.ac.ebi.ddi.ddifileservice.services.IFileSystem;
import uk.ac.ebi.ddi.ddifileservice.type.CloseableFile;
import uk.ac.ebi.ddi.service.db.model.dataset.Dataset;
import uk.ac.ebi.ddi.service.db.model.dataset.DatasetFile;
import uk.ac.ebi.ddi.service.db.service.dataset.DatasetFileService;
import uk.ac.ebi.ddi.service.db.service.dataset.IDatasetService;
import uk.ac.ebi.ddi.task.ddidatasetimporter.configuration.DatasetImportTaskProperties;
import uk.ac.ebi.ddi.task.ddidatasetimporter.services.DatabaseImporterService;
import uk.ac.ebi.ddi.task.ddidatasetimporter.services.DatasetImporterService;
import uk.ac.ebi.ddi.task.ddidatasetimporter.utils.Constants;
import uk.ac.ebi.ddi.task.ddidatasetimporter.utils.DatasetUtils;
import uk.ac.ebi.ddi.task.ddidatasetimporter.utils.EntryUtils;
import uk.ac.ebi.ddi.xml.validator.exception.DDIException;
import uk.ac.ebi.ddi.xml.validator.parser.OmicsXMLFile;
import uk.ac.ebi.ddi.xml.validator.parser.model.Entry;

import java.io.File;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static uk.ac.ebi.ddi.ddidomaindb.dataset.DSField.Additional.DATASET_FILE;
import static uk.ac.ebi.ddi.ddidomaindb.dataset.DSField.Additional.SUBMITTER_KEYWORDS;
import static uk.ac.ebi.ddi.service.db.utils.Constants.FROM_DATASET_IMPORT;

@SpringBootApplication()
public class DdiDatasetImporterApplication implements CommandLineRunner {

    @Autowired
    private IFileSystem fileSystem;

    @Autowired
    private DatasetImportTaskProperties taskProperties;

    @Autowired
    private DatasetImporterService datasetImporterService;

    @Autowired
    private DatabaseImporterService databaseImporterService;

    @Autowired
    private IDatasetService datasetService;

    @Autowired
    private DatasetFileService datasetFileService;

    private boolean isDatabaseUpdated = false;

    private CopyOnWriteArrayList<Map.Entry<String, String>> threadSafeList = new CopyOnWriteArrayList<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(DdiFileServiceApplication.class);


    public static void main(String[] args) {
        SpringApplication.run(DdiDatasetImporterApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        List<String> files = fileSystem.listFilesFromFolder(taskProperties.getInputDirectory());
        if (files.isEmpty()) {
            LOGGER.warn("Input directory is empty");
            return;
        }

        for (String filePath : files) {
            try (CloseableFile file = fileSystem.getFile(filePath)) {
                LOGGER.info("Processing file {}", filePath);
                process(file);
            } catch (Exception e) {
                LOGGER.error("Exception occurred when processing file {}, ", filePath, e);
            }
        }

        Set<String> databases = threadSafeList.parallelStream().map(Map.Entry::getValue).collect(Collectors.toSet());
        CopyOnWriteArrayList<Map.Entry<List<Dataset>, String>> datasets = new CopyOnWriteArrayList<>();
        databases.parallelStream().forEach(database -> datasets.add(
                new AbstractMap.SimpleEntry<>(datasetService.readDatasetHashCode(database), database)));

        CopyOnWriteArrayList<Dataset> toBeRemoved = new CopyOnWriteArrayList<>();
        datasets.parallelStream().forEach(x -> x.getKey().parallelStream().forEach(ds -> {
            Map.Entry<String, String> pair = new AbstractMap.SimpleEntry<>(ds.getAccession(), ds.getDatabase());
            if (!threadSafeList.contains(pair)) {
                toBeRemoved.add(ds);
            }
        }));

        if (!taskProperties.isUpdateStatus()) {
            toBeRemoved.forEach(x -> datasetImporterService.updateDeleteStatus(x));
        }
    }

    private void process(File file) throws DDIException {
        OmicsXMLFile omicsXMLFile = new OmicsXMLFile(file);
        updateDatabaseInfo(omicsXMLFile);
        List<Entry> entries = omicsXMLFile.getAllEntries();
        for (Entry dataEntry : entries) {
            String dbName = DatasetUtils.getDbName(dataEntry, omicsXMLFile.getDatabaseName());

            dataEntry.getAdditionalFieldValues(SUBMITTER_KEYWORDS.key())
                    .stream()
                    .flatMap(dt -> Arrays.stream(dt.split(Constants.SEMI_COLON_TOKEN)))
                    .distinct()
                    .forEach(tr -> dataEntry.addAdditionalField(SUBMITTER_KEYWORDS.key(), tr));

            // Update list of files
            List<String> files = EntryUtils.removeField(dataEntry, DATASET_FILE.key());
            datasetFileService.deleteAll(dataEntry.getId(), dbName, FROM_DATASET_IMPORT);
            datasetFileService.saveAll(files.stream()
                    .map(x -> new DatasetFile(dataEntry.getId(), dbName, x, FROM_DATASET_IMPORT))
                    .collect(Collectors.toList()));

            LOGGER.debug("inserting: " + dataEntry.getId() + " " + dbName + "");

            datasetImporterService.insertDataset(dataEntry, dbName);
            threadSafeList.add(new AbstractMap.SimpleEntry<>(dataEntry.getId(), dbName));
            LOGGER.debug("Dataset: " + dataEntry.getId() + " " + dbName + "has been added");
        }
    }

    private synchronized void updateDatabaseInfo(OmicsXMLFile file) {
        if (!isDatabaseUpdated) {
            databaseImporterService.updateDatabase(file.getDatabaseName(),
                    file.getDescription(), file.getReleaseDate(), file.getRelease(), null, null);
            isDatabaseUpdated = true;
        }
    }
}
