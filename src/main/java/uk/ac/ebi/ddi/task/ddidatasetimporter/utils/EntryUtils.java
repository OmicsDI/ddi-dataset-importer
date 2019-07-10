package uk.ac.ebi.ddi.task.ddidatasetimporter.utils;

import uk.ac.ebi.ddi.xml.validator.parser.model.AdditionalFields;
import uk.ac.ebi.ddi.xml.validator.parser.model.Entry;
import uk.ac.ebi.ddi.xml.validator.parser.model.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EntryUtils {

    /**
     * Remove the key from entry fields, return the list of values from the deleted keys
     * @param entry
     * @param name
     * @return
     */
    public static List<String> removeField(Entry entry, String name) {
        List<Field> fieldToBeDeleted = new ArrayList<>();
        AdditionalFields additionalFields = entry.getAdditionalFields();
        if (additionalFields != null) {
            for (Field field : additionalFields.getField()) {
                if (field.getName().equalsIgnoreCase(name)) {
                    fieldToBeDeleted.add(field);
                }
            }
        }
        List<String> values = fieldToBeDeleted.stream().map(Field::getValue).collect(Collectors.toList());
        for (Field field : fieldToBeDeleted) {
            entry.getAdditionalFields().getField().remove(field);
        }
        return values;
    }
}
