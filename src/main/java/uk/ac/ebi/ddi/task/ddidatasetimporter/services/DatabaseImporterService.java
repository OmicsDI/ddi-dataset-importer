package uk.ac.ebi.ddi.task.ddidatasetimporter.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.ac.ebi.ddi.service.db.model.dataset.Database;
import uk.ac.ebi.ddi.service.db.service.dataset.IDatabaseService;

import java.util.List;

@Service
public class DatabaseImporterService {

    @Autowired
    IDatabaseService databaseService;

    public void updateDatabase(String name, String description, String releaseDate, String releaseTag,
                               List<String> omicsType, String url) {
        Database database = new Database(name, description, releaseDate, releaseTag, omicsType, url);
        Database existingDatabase = databaseService.read(name);
        if (existingDatabase != null) {
            databaseService.update(existingDatabase.get_id(), database);
        } else {
            databaseService.save(database);
        }
    }
}
