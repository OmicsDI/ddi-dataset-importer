package uk.ac.ebi.ddi.task.ddidatasetimporter.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.ac.ebi.ddi.service.db.model.dataset.Dataset;
import uk.ac.ebi.ddi.service.db.model.dataset.DatasetStatus;
import uk.ac.ebi.ddi.service.db.service.dataset.IDatasetService;
import uk.ac.ebi.ddi.service.db.service.dataset.IDatasetStatusService;
import uk.ac.ebi.ddi.service.db.utils.DatasetCategory;
import uk.ac.ebi.ddi.task.ddidatasetimporter.utils.DatasetUtils;
import uk.ac.ebi.ddi.task.ddidatasetimporter.utils.DateUtils;
import uk.ac.ebi.ddi.xml.validator.parser.model.Entry;

@Service
public class DatasetImporterService {

    @Autowired
    IDatasetService datasetService;

    @Autowired
    IDatasetStatusService statusService;

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetImporterService.class);

    public void insertDataset(Entry dataset, String databaseName) {
        Dataset dbDataset = DatasetUtils.transformEntryDataset(dataset, databaseName);
        DatasetUtils.replaceTextCase(dbDataset);
        Dataset currentDataset = datasetService.read(dbDataset.getAccession(), dbDataset.getDatabase());

        if (currentDataset != null) {
            if (currentDataset.getInitHashCode() != dbDataset.getInitHashCode()) {
                LOGGER.info("Init hashcode of " + dbDataset.getAccession() + " is changed, previous: {}, current: {}",
                        currentDataset.getInitHashCode(), dbDataset.getInitHashCode());
                updateDataset(currentDataset, dbDataset);
            }
        } else {
            LOGGER.info("Inserting dataset as dataset is not available, {}", dbDataset.getAccession());
            insertDataset(dbDataset);
        }
    }

    private void insertDataset(Dataset dbDataset) {
        dbDataset = datasetService.save(dbDataset);
        if (dbDataset.getId() != null) {
            statusService.save(new DatasetStatus(dbDataset.getAccession(), dbDataset.getDatabase(),
                    dbDataset.getInitHashCode(), DateUtils.getDate(), DatasetCategory.INSERTED.getType())
            );
        }
    }

    private void updateDataset(Dataset currentDataset, Dataset newDataset) {

        Dataset dbDataset = datasetService.update(currentDataset.getId(), newDataset);
        dbDataset.setInitHashCode(newDataset.getInitHashCode());
        datasetService.save(dbDataset);
        if (dbDataset.getId() != null) {
            statusService.save(new DatasetStatus(dbDataset.getAccession(), dbDataset.getDatabase(),
                    dbDataset.getInitHashCode(), DateUtils.getDate(), DatasetCategory.INSERTED.getType()));
        }
    }

    public void updateDeleteStatus(Dataset dataset) {
        Dataset existingDataset = datasetService.read(dataset.getId());
        updateStatus(existingDataset, DatasetCategory.DELETED.getType());
    }

    private void updateStatus(Dataset dbDataset, String status) {
        dbDataset.setCurrentStatus(status);
        dbDataset = datasetService.update(dbDataset.getId(), dbDataset);
        if (dbDataset.getId() != null) {
            statusService.save(new DatasetStatus(dbDataset.getAccession(), dbDataset.getDatabase(),
                    dbDataset.getInitHashCode(), DateUtils.getDate(), status)
            );
        }
    }
}
