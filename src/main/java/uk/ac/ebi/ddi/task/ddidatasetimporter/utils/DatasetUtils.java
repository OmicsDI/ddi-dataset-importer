package uk.ac.ebi.ddi.task.ddidatasetimporter.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.ddi.ddidomaindb.dataset.DSField;
import uk.ac.ebi.ddi.service.db.model.dataset.Dataset;
import uk.ac.ebi.ddi.service.db.utils.DatasetCategory;
import uk.ac.ebi.ddi.xml.validator.parser.model.Date;
import uk.ac.ebi.ddi.xml.validator.parser.model.Entry;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DatasetUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetUtils.class);

    /**
     * This function with use a database as a fixed name. That means that the user will use
     * the name provided in the function and not the one provided in the File.
     * @param dataset Dataset Entry from the XML
     * @param databaseName The database Name
     * @return Dataset from the dtabase.
     */
    public static Dataset transformEntryDataset(Entry dataset, String databaseName) {

        Map<String, Set<String>> dates = new HashMap<>();
        Map<String, Set<String>> crossReferences = new HashMap<>();
        Map<String, Set<String>> additionals = new HashMap<>();
        try {
            if (dataset.getName() == null) {
                LOGGER.error("Exception occurred, entry with id name value is not there, acc: {}", dataset.getAcc());
            }
            if (dataset.getDates() != null) {
                dates = dataset.getDates().getDate().parallelStream()
                        .collect(Collectors.groupingBy(
                                Date::getType,
                                Collectors.mapping(Date::getValue, Collectors.toSet())));
            }
            crossReferences = new HashMap<>();
            if (dataset.getCrossReferences() != null && dataset.getCrossReferences().getRef() != null) {
                crossReferences = dataset.getCrossReferences().getRef()
                        .stream().parallel()
                        .collect(Collectors.groupingBy(
                                x -> x.getDbname().trim(),
                                Collectors.mapping(x -> x.getDbkey().trim(), Collectors.toSet())));
            }
            if (dataset.getAdditionalFields() != null) {
                additionals = dataset.getAdditionalFields().getField()
                        .stream().parallel()
                        .collect(Collectors.groupingBy(
                                x -> x.getName().trim(),
                                Collectors.mapping(x -> x.getValue().trim(), Collectors.toSet())));
            }
            Set<String> repositories = additionals.get(DSField.Additional.REPOSITORY.getName());
            if (null == repositories || repositories.size() < 1) {
                //AZ:this code overrides additonal.repository. why? wrapped by if
                //** Rewrite the respoitory with the name we would like to handle ***/
                Set<String> databases = new HashSet<>();
                databases.add(databaseName);
                additionals.put(DSField.Additional.REPOSITORY.getName(), databases);
            }
        } catch (Exception ex) {
            LOGGER.error("Exception occured in transformEntryDataset entry with id {}, ", dataset.getId(), ex);
        }
        return new Dataset(dataset.getId(), databaseName,
                dataset.getName() != null ? dataset.getName().getValue() : "",
                dataset.getDescription(), dates, additionals, crossReferences, DatasetCategory.INSERTED);
    }

    public static Dataset replaceTextCase(Dataset existingDataset) {
        if (existingDataset.getAdditional().get(DSField.Additional.DISEASE_FIELD.getName()) != null) {
            Set<String> diseases = existingDataset.getAdditional().get(DSField.Additional.DISEASE_FIELD.getName());
            Set<String> updatedDisease =  diseases.parallelStream()
                    .map(x -> toTitleCase(x.toLowerCase())).collect(Collectors.toSet());
            existingDataset.addAdditional(DSField.Additional.DISEASE_FIELD.getName(), updatedDisease);
        }
        if (existingDataset.getAdditional().get(DSField.Additional.SPECIE_FIELD.getName()) != null) {
            Set<String> diseases = existingDataset.getAdditional().get(DSField.Additional.SPECIE_FIELD.getName());
            Set<String> updatedSpecies =  diseases.parallelStream()
                    .map(x -> toTitleCase(x.toLowerCase())).collect(Collectors.toSet());
            existingDataset.addAdditional(DSField.Additional.SPECIE_FIELD.getName(), updatedSpecies);
        }
        if (existingDataset.getAdditional().get(DSField.Additional.TISSUE_FIELD.getName()) != null) {
            Set<String> diseases = existingDataset.getAdditional().get(DSField.Additional.TISSUE_FIELD.getName());
            Set<String> updatedTissue = diseases.parallelStream()
                    .map(x -> toTitleCase(x.toLowerCase())).collect(Collectors.toSet());
            existingDataset.addAdditional(DSField.Additional.TISSUE_FIELD.getName(), updatedTissue);
        }
        return existingDataset;
    }

    public static String toTitleCase(String input) {
        StringBuilder titleCase = new StringBuilder();
        boolean nextTitleCase = true;
        for (char c : input.toCharArray()) {
            if (Character.isSpaceChar(c)) {
                nextTitleCase = true;
            } else if (nextTitleCase) {
                c = Character.toTitleCase(c);
                nextTitleCase = false;
            }
            titleCase.append(c);
        }
        return titleCase.toString();
    }

    public static String getDbName(Entry dataset, String originalDBName) {
        String dbName = originalDBName != null ? originalDBName : "NA";
        if ("".equals(dbName)) {
            return dataset.getRepository() != null ? dataset.getRepository() : "";
        }
        return dbName;
    }
}
