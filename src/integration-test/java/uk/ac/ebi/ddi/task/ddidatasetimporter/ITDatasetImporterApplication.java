package uk.ac.ebi.ddi.task.ddidatasetimporter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ddi.ddifileservice.services.IFileSystem;
import uk.ac.ebi.ddi.service.db.model.dataset.Dataset;
import uk.ac.ebi.ddi.service.db.service.dataset.DatasetFileService;
import uk.ac.ebi.ddi.service.db.service.dataset.IDatabaseService;
import uk.ac.ebi.ddi.service.db.service.dataset.IDatasetService;
import uk.ac.ebi.ddi.task.ddidatasetimporter.configuration.DatasetImportTaskProperties;

import java.io.File;
import java.util.List;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = DdiDatasetImporterApplication.class,
		initializers = ConfigFileApplicationContextInitializer.class)
@TestPropertySource(properties = {
		"file.provider=local",
		"importer.database_name=ArrayExpress",
		"importer.input_directory=/tmp/importer"
})
public class ITDatasetImporterApplication {

	@Autowired
	private DatasetImportTaskProperties taskProperties;

	@Autowired
	private IFileSystem fileSystem;

	@Autowired
	private IDatabaseService databaseService;

	@Autowired
	private IDatasetService datasetService;

	@Autowired
	private DdiDatasetImporterApplication application;

	@Autowired
	private DatasetFileService datasetFileService;

	@Before
	public void setUp() throws Exception {
		File file = new File(taskProperties.getInputDirectory());
		file.mkdirs();

		File importFile = new File(getClass().getClassLoader().getResource("ARRAY_EXPRESS_EBE_1.xml").getFile());
		fileSystem.copyFile(importFile, taskProperties.getInputDirectory() + "/ARRAY_EXPRESS_EBE_1.xml");
		importFile = new File(getClass().getClassLoader().getResource("BIOMODELS_EBEYE_66.xml").getFile());
		fileSystem.copyFile(importFile, taskProperties.getInputDirectory() + "/BIOMODELS_EBEYE_66.xml");
	}

	@Test
	public void contextLoads() throws Exception {
		application.run();
		List<Dataset> datasets = datasetService.readDatasetHashCode(taskProperties.getDatabaseName());

		Assert.assertEquals(200, datasets.size());

		Dataset dataset = datasetService.read("E-ATMX-10", taskProperties.getDatabaseName());
		Assert.assertNotNull(dataset);
		Assert.assertEquals("E-ATMX-10", dataset.getAccession());
		Assert.assertEquals(2, dataset.getDates().size());
		Assert.assertEquals(11, dataset.getAdditional().size());
		Assert.assertEquals("Inserted", dataset.getCurrentStatus());
		Assert.assertTrue(dataset.getDescription().contains("Plants with genotypes"));

		List<String> files = datasetFileService.getFiles("E-ATMX-19", taskProperties.getDatabaseName());
		Assert.assertEquals(17, files.size());
	}

	@After
	public void tearDown() throws Exception {
		fileSystem.cleanDirectory(taskProperties.getInputDirectory());
	}
}
