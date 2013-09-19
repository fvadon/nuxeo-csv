/*
 * (C) Copyright 2012 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Thomas Roger
 */

package org.nuxeo.ecm.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileFilter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.common.utils.FileUtils;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.api.impl.blob.FileBlob;
import org.nuxeo.ecm.core.storage.sql.ra.PoolingRepositoryFactory;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.TransactionalFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.transaction.TransactionHelper;

import com.google.inject.Inject;

/**
 * @author <a href="mailto:troger@nuxeo.com">Thomas Roger</a>
 * @since 5.7
 */
@RunWith(FeaturesRunner.class)
@Features({ TransactionalFeature.class, CoreFeature.class })
@Deploy({ "studio.extensions.ecp","org.nuxeo.ecm.csv", "org.nuxeo.runtime.datasource" })
@RepositoryConfig(repositoryFactoryClass = PoolingRepositoryFactory.class, cleanup = Granularity.METHOD)
public class TestCSVImport {

    @Inject
    protected CoreSession session;

    @Inject
    protected CSVImporter csvImporter;

    @Inject
    protected WorkManager workManager;

    @Before
    public void clearWorkQueue() {
        workManager.clearCompletedWork(0);
    }

    private Blob getCSVFile(String name) {
        File file = new File(FileUtils.getResourcePathFromContext(name));
        Blob blob = new FileBlob(file);
        blob.setFilename(file.getName());
        return blob;
    }

    @Test
    public void shouldCreateAllDocuments() throws InterruptedException,
            ClientException {
        CSVImporterOptions options = CSVImporterOptions.DEFAULT_OPTIONS;
        TransactionHelper.commitOrRollbackTransaction();

        CSVImportId importId = csvImporter.launchImport(session, "/",
                getCSVFile("ExtraSimpleNoFileMiniNoNameNoType.csv"), options);

        workManager.awaitCompletion(10, TimeUnit.SECONDS);
        TransactionHelper.startTransaction();
        List<CSVImportLog> importLogs = csvImporter.getImportLogs(importId);
        assertEquals(5, importLogs.size());
        CSVImportLog importLog = importLogs.get(0);
        assertEquals(1, importLog.getLine());
        assertEquals(CSVImportLog.Status.SUCCESS, importLog.getStatus());
        importLog = importLogs.get(1);
        assertEquals(2, importLog.getLine());
        assertEquals(CSVImportLog.Status.SUCCESS, importLog.getStatus());

        assertTrue(session.exists(new PathRef("/FOOBAR 6.D.20100416.9")));
        DocumentModel doc = session.getDocument(new PathRef("/FOOBAR 6.D.20100416.9"));
        assertEquals("FOOBAR 6.D.20100416.9", doc.getTitle());
        assertEquals("D", doc.getPropertyValue("bg:IDSubject"));
    }
    
    @Test
    public void shouldImportFilesFromPattern() throws InterruptedException, ClientException {
    	File ressourceRoot = FileUtils.getResourceFileFromContext("datas");
    	File[] files = FileUtils.findFiles(ressourceRoot, "A*.pdf", false);
    	assertEquals(1,files.length);
    	Framework.getProperties().put("nuxeo.csv.blobs.folder", ressourceRoot.getPath());
    	
    	CSVImporterOptions options = CSVImporterOptions.DEFAULT_OPTIONS;
        TransactionHelper.commitOrRollbackTransaction();

        CSVImportId importId = csvImporter.launchImport(session, "/",
                getCSVFile("AutoFileMiniNoNameNoType.csv"), options);

        workManager.awaitCompletion(10, TimeUnit.SECONDS);
        TransactionHelper.startTransaction();
        List<CSVImportLog> importLogs = csvImporter.getImportLogs(importId);
        assertEquals(3, importLogs.size());
        CSVImportLog importLog = importLogs.get(0);
        assertEquals(1, importLog.getLine());
        assertEquals(CSVImportLog.Status.SUCCESS, importLog.getStatus());
        assertTrue(session.exists(new PathRef("/TEST 2.D.20100416.0")));
        DocumentModel doc = session.getDocument(new PathRef("/TEST 2.D.20100416.0"));
        assertEquals("TEST 2.D.20100416.0", doc.getTitle());
        assertEquals("yes",doc.getPropertyValue("filing:autoimport"));  
    }
    
    @Test
    public void shouldSkipExistingDocuments() throws InterruptedException,
            ClientException {
        DocumentModel doc = session.createDocumentModel("/", "mynote", "Note");
        doc.setPropertyValue("dc:title", "Existing Note");
        session.createDocument(doc);
        TransactionHelper.commitOrRollbackTransaction();

        CSVImporterOptions options = new CSVImporterOptions.Builder().updateExisting(
                false).build();
        CSVImportId importId = csvImporter.launchImport(session, "/",
                getCSVFile("docs_ok.csv"), options);

        workManager.awaitCompletion(10, TimeUnit.SECONDS);
        TransactionHelper.startTransaction();

        List<CSVImportLog> importLogs = csvImporter.getImportLogs(importId);
        assertEquals(2, importLogs.size());
        CSVImportLog importLog = importLogs.get(0);
        assertEquals(1, importLog.getLine());
        assertEquals(CSVImportLog.Status.SUCCESS, importLog.getStatus());
        assertEquals("Document created", importLog.getMessage());
        importLog = importLogs.get(1);
        assertEquals(2, importLog.getLine());
        assertEquals(CSVImportLog.Status.SKIPPED, importLog.getStatus());
        assertEquals("Document already exists", importLog.getMessage());

        assertTrue(session.exists(new PathRef("/myfile")));
        doc = session.getDocument(new PathRef("/myfile"));
        assertEquals("My File", doc.getTitle());
        assertEquals("a simple file", doc.getPropertyValue("dc:description"));

        assertTrue(session.exists(new PathRef("/mynote")));
        doc = session.getDocument(new PathRef("/mynote"));
        assertEquals("Existing Note", doc.getTitle());
        assertFalse("a simple note".equals(doc.getPropertyValue("dc:description")));
    }

    @Test
    public void shouldStoreLineWithErrors() throws InterruptedException,
            ClientException {
        CSVImporterOptions options = new CSVImporterOptions.Builder().updateExisting(
                false).build();
        TransactionHelper.commitOrRollbackTransaction();
        CSVImportId importId = csvImporter.launchImport(session, "/",
                getCSVFile("docs_not_ok.csv"), options);
        workManager.awaitCompletion(10, TimeUnit.SECONDS);
        TransactionHelper.startTransaction();

        List<CSVImportLog> importLogs = csvImporter.getImportLogs(importId);
        assertEquals(4, importLogs.size());

        CSVImportLog importLog = importLogs.get(0);
        assertEquals(1, importLog.getLine());
        assertEquals(CSVImportLog.Status.ERROR, importLog.getStatus());
        assertEquals(
                "Unable to convert field 'dc:issued' with value '10012010'",
                importLog.getMessage());
        importLog = importLogs.get(1);
        assertEquals(2, importLog.getLine());
        assertEquals(CSVImportLog.Status.SUCCESS, importLog.getStatus());
        assertEquals("Document created", importLog.getMessage());
        importLog = importLogs.get(2);
        assertEquals(3, importLog.getLine());
        assertEquals(CSVImportLog.Status.ERROR, importLog.getStatus());
        assertEquals("The type 'NotExistingType' does not exist",
                importLog.getMessage());
        importLog = importLogs.get(3);
        assertEquals(4, importLog.getLine());
        assertEquals(CSVImportLog.Status.SUCCESS, importLog.getStatus());
        assertEquals("Document created", importLog.getMessage());

        assertFalse(session.exists(new PathRef("/myfile")));
        assertTrue(session.exists(new PathRef("/mynote")));
        assertFalse(session.exists(new PathRef("/nonexisting")));
        assertTrue(session.exists(new PathRef("/mynote2")));
    }

}
