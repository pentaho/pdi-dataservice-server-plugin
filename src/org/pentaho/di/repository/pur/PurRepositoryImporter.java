/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.imp.ImportRules;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.job.JobEntryJob;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.IRepositoryImporter;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryExportSaxParser;
import org.pentaho.di.repository.RepositoryImportFeedbackInterface;
import org.pentaho.di.repository.RepositoryImportLocation;
import org.pentaho.di.repository.RepositoryImporter;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mapping.MappingMeta;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXParseException;

public class PurRepositoryImporter implements IRepositoryImporter, java.io.Serializable {

  private static final long serialVersionUID = 2853810493291696227L; /* EESOURCE: UPDATE SERIALVERUID */
  private static Class<?>              PKG           = PurRepositoryImporter.class; 

  private PurRepository rep;
  private LogChannelInterface log;

  private RepositoryDirectoryInterface baseDirectory;
  
  private RepositoryDirectoryInterface root;
  
  private boolean overwrite;
  private boolean askOverwrite  = true;

  private String versionComment;

  private boolean continueOnError;
  
  private String transDirOverride = null;
  private String jobDirOverride = null;

  private ImportRules importRules;
  
  private List<RepositoryObject> referencingObjects;

  private List<Exception> exceptions;

  public PurRepositoryImporter(PurRepository repository) {
      this.log = new LogChannel("Repository import"); //$NON-NLS-1$
      this.importRules = new ImportRules();
      this.rep = repository;
  }
  
  public synchronized void importAll(RepositoryImportFeedbackInterface feedback, String fileDirectory, String[] filenames, RepositoryDirectoryInterface baseDirectory, boolean overwrite, boolean continueOnError, String versionComment) {
    this.baseDirectory = baseDirectory;
    this.overwrite = overwrite;
    this.continueOnError = continueOnError;
    this.versionComment = versionComment;

    referencingObjects=new ArrayList<RepositoryObject>();
    
    feedback.setLabel(BaseMessages.getString(PKG, "PurRepositoryImporter.ImportXML.Label"));
    try {
      
      RepositoryImportLocation.setRepositoryImportLocation(baseDirectory);

      for (int ii = 0; ii < filenames.length; ++ii) {

        final String filename = ((fileDirectory != null) && (fileDirectory.length() > 0)) ? fileDirectory + Const.FILE_SEPARATOR + filenames[ii] : filenames[ii];
        if (log.isBasic())
          log.logBasic("Import objects from XML file [" + filename + "]"); //$NON-NLS-1$ //$NON-NLS-2$
        feedback.addLog(BaseMessages.getString(PKG, "PurRepositoryImporter.WhichFile.Log", filename));

        // To where?
        feedback.setLabel(BaseMessages.getString(PKG, "PurRepositoryImporter.WhichDir.Label"));

        // Read it using SAX...
        //
        try {
          RepositoryExportSaxParser parser = new RepositoryExportSaxParser(filename, feedback);          
          parser.parse(this);
        }
        catch (FileNotFoundException fnfe) {
          addException(fnfe);
          feedback.showError(BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorGeneral.Title"), 
                BaseMessages.getString(PKG, "PurRepositoryImporter.FileNotFound.Message", filename), fnfe);
          
        }
        catch (SAXParseException spe) {
          addException(spe);
          feedback.showError(BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorGeneral.Title"), 
                 BaseMessages.getString(PKG, "PurRepositoryImporter.ParseError.Message", filename), spe);
        }
        catch(Exception e) {
          addException(e);
          feedback.showError(BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorGeneral.Title"), 
              BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorGeneral.Message"), e);
        }        
      }
      
      // Correct those jobs and transformations that contain references to other objects.
      //
      for (RepositoryObject ro : referencingObjects) {
        if (ro.getObjectType()==RepositoryObjectType.TRANSFORMATION) {
          TransMeta transMeta = rep.loadTransformation(ro.getObjectId(), null);
          try {
            transMeta.lookupRepositoryReferences(rep);
          } catch (KettleException e) {
            // log and continue; might fail from exports performed before PDI-5294
            feedback.addLog(BaseMessages.getString(PKG, "PurRepositoryImporter.LookupRepoRefsError.Log", transMeta.getName()));
          }
          rep.save(transMeta, "import object reference specification", null);
        }
        if (ro.getObjectType()==RepositoryObjectType.JOB) {
          JobMeta jobMeta = rep.loadJob(ro.getObjectId(), null);
          try {
            jobMeta.lookupRepositoryReferences(rep);
          } catch (KettleException e) {
            // log and continue; might fail from exports performed before PDI-5294
            feedback.addLog(BaseMessages.getString(PKG, "PurRepositoryImporter.LookupRepoRefsError.Log", jobMeta.getName()));
          }
          rep.save(jobMeta, "import object reference specification", null);
        }
      }

      feedback.addLog(BaseMessages.getString(PKG, "PurRepositoryImporter.ImportFinished.Log"));
    } catch (Exception e) {
      addException(e);
      feedback.showError(BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorGeneral.Title"), 
          BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorGeneral.Message"), e);
    } finally {
      RepositoryImportLocation.setRepositoryImportLocation(null); // set to null
                                                                  // when done!
    }
  }

  public void addLog(String line) {
    log.logBasic(line);
  }

  public void setLabel(String labelText) {
    log.logBasic(labelText);
  }

  public boolean transOverwritePrompt(TransMeta transMeta) {
    return overwrite;
  }
  
  public boolean jobOverwritePrompt(JobMeta jobMeta) {
    return overwrite;
  }

  public void updateDisplay() {
  }
  
  public void showError(String title, String message, Exception e) {
    log.logError(message, e);
  }

  private void replaceSharedObjects(TransMeta transMeta) throws KettleException {
    Map<RepositoryObjectType, List<? extends SharedObjectInterface>> sharedObjectsByType = rep.loadAndCacheSharedObjects();
    
      // Database...
      //
      for (SharedObjectInterface sharedObject : sharedObjectsByType.get(RepositoryObjectType.DATABASE)) {
        DatabaseMeta databaseMeta = (DatabaseMeta) sharedObject;
        int index = transMeta.indexOfDatabase(databaseMeta);
        if (index<0) {
          transMeta.addDatabase(databaseMeta);
        } else {
          DatabaseMeta imported = transMeta.getDatabase(index);
          if (overwrite) {
            // Preserve the object id so we can update without having to look up the id
            imported.setObjectId(databaseMeta.getObjectId());
            imported.setChanged();
          } else {
            imported.replaceMeta(databaseMeta);
            // replaceMeta sets the changed flag for the existing (unchanged - from repo) meta
            imported.clearChanged();
          }
        }
      }

      // Slave Server...
      //
      for (SharedObjectInterface sharedObject : sharedObjectsByType.get(RepositoryObjectType.SLAVE_SERVER)) {
        SlaveServer slaveServer = (SlaveServer) sharedObject;
        
        int index = transMeta.getSlaveServers().indexOf(slaveServer);
        if (index<0) {
          transMeta.getSlaveServers().add(slaveServer);
        } else {
          SlaveServer imported = transMeta.getSlaveServers().get(index);
          if (overwrite) {
            // Preserve the object id so we can update without having to look up the id
            imported.setObjectId(slaveServer.getObjectId());
            imported.setChanged();
          } else {
            imported.replaceMeta(slaveServer);
            // replaceMeta sets the changed flag for the existing (unchanged - from repo) meta
            imported.clearChanged();
          }
        }
      }

      // Cluster Schema...
      //
      for (SharedObjectInterface sharedObject : sharedObjectsByType.get(RepositoryObjectType.CLUSTER_SCHEMA)) {
        ClusterSchema clusterSchema = (ClusterSchema) sharedObject;
        
        int index = transMeta.getClusterSchemas().indexOf(clusterSchema);
        if (index<0) {
          transMeta.getClusterSchemas().add(clusterSchema);
        } else {
          ClusterSchema imported = transMeta.getClusterSchemas().get(index);
          if (overwrite) {
            // Preserve the object id so we can update without having to look up the id
            imported.setObjectId(clusterSchema.getObjectId());
            imported.setChanged();
          } else {
            imported.replaceMeta(clusterSchema);
            // replaceMeta sets the changed flag for the existing (unchanged - from repo) meta
            imported.clearChanged();
          }
        }
      }

      // Partition Schema...
      //
      for (SharedObjectInterface sharedObject : sharedObjectsByType.get(RepositoryObjectType.PARTITION_SCHEMA)) {
        PartitionSchema partitionSchema = (PartitionSchema) sharedObject;
        
        int index = transMeta.getPartitionSchemas().indexOf(partitionSchema);
        if (index<0) {
          transMeta.getPartitionSchemas().add(partitionSchema);
        } else {
          PartitionSchema imported = transMeta.getPartitionSchemas().get(index);
          if (overwrite) {
            // Preserve the object id so we can update without having to look up the id
            imported.setObjectId(partitionSchema.getObjectId());
            imported.setChanged();
          } else {
            imported.replaceMeta(partitionSchema);
            // replaceMeta sets the changed flag for the existing (unchanged - from repo) meta
            imported.clearChanged();
          }
        }
      }

  }
  
  private void replaceSharedObjects(JobMeta transMeta) throws KettleException {
    Map<RepositoryObjectType, List<? extends SharedObjectInterface>> sharedObjectsByType = rep.loadAndCacheSharedObjects();
    
      // Database...
      //
    for (SharedObjectInterface sharedObject : sharedObjectsByType.get(RepositoryObjectType.DATABASE)) {
        DatabaseMeta databaseMeta = (DatabaseMeta) sharedObject;
        int index = transMeta.indexOfDatabase(databaseMeta);
        if (index<0) {
          transMeta.addDatabase(databaseMeta);
        } else {
          DatabaseMeta imported = transMeta.getDatabase(index);
          if (overwrite) {
            // Preserve the object id so we can update without having to look up the id
            imported.setObjectId(databaseMeta.getObjectId());
            imported.setChanged();
          } else {
            imported.replaceMeta(databaseMeta);
            //replaceMeta sets the changed flag for the existing (unchanged - from repo) meta
            imported.clearChanged();
          }
        }
      }

      // Slave Server...
      //
    for (SharedObjectInterface sharedObject : sharedObjectsByType.get(RepositoryObjectType.SLAVE_SERVER)) {
        SlaveServer slaveServer = (SlaveServer) sharedObject;
        
        int index = transMeta.getSlaveServers().indexOf(slaveServer);
        if (index<0) {
          transMeta.getSlaveServers().add(slaveServer);
        } else {
          SlaveServer imported = transMeta.getSlaveServers().get(index);
          if (overwrite) {
            // Preserve the object id so we can update without having to look up the id
            imported.setObjectId(slaveServer.getObjectId());
            imported.setChanged();
          } else {
            imported.replaceMeta(slaveServer);
            // replaceMeta sets the changed flag for the existing (unchanged - from repo) meta
            imported.clearChanged();
          }
        }
      }
  }


  private void patchMappingSteps(TransMeta transMeta) {
    for (StepMeta stepMeta : transMeta.getSteps()) {
      if (stepMeta.isMapping()) {
        MappingMeta mappingMeta = (MappingMeta) stepMeta.getStepMetaInterface();
        if (mappingMeta.getSpecificationMethod() == ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME) {
          String newPath = baseDirectory.getPath();
          String extraPath = mappingMeta.getDirectoryPath();
          if (newPath.endsWith("/") && extraPath.startsWith("/")) {
            newPath=newPath.substring(0,newPath.length()-1);
          } else if (!newPath.endsWith("/") && !extraPath.startsWith("/")) {
            newPath+="/";
          } else if (extraPath.equals("/")) {
            extraPath="";
          }
          mappingMeta.setDirectoryPath(newPath+extraPath);
        }
      }
    }
  }

  private void patchJobEntries(JobMeta jobMeta) {
    for (JobEntryCopy copy : jobMeta.getJobCopies()) {
      if (copy.isTransformation()) {
        JobEntryTrans entry = (JobEntryTrans) copy.getEntry();
        if (entry.getSpecificationMethod() == ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME) {
          String newPath = baseDirectory.getPath();
          String extraPath = Const.NVL(entry.getDirectory(), "/");
          if (newPath.endsWith("/") && extraPath.startsWith("/")) {
            newPath=newPath.substring(0,newPath.length()-1);
          } else if (!newPath.endsWith("/") && !extraPath.startsWith("/")) {
            newPath+="/";
          } else if (extraPath.equals("/")) {
            extraPath="";
          }
          entry.setDirectory(newPath+extraPath);
        }
      }
      if (copy.isJob()) {
        JobEntryJob entry = (JobEntryJob) copy.getEntry();
        if (entry.getSpecificationMethod() == ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME) {
          String newPath = baseDirectory.getPath();
          String extraPath = Const.NVL(entry.getDirectory(), "/");
          if (newPath.endsWith("/") && extraPath.startsWith("/")) {
            newPath=newPath.substring(0,newPath.length()-1);
          } else if (!newPath.endsWith("/") && !extraPath.startsWith("/")) {
            newPath+="/";
          } else if (extraPath.equals("/")) {
            extraPath="";
          }
          entry.setDirectory(newPath+extraPath);
        }
      }
    }
  }

  /**
   * 
   * @param transnode
   *          The XML DOM node to read the transformation from
   * @return false if the import should be canceled.
   * @throws KettleException
   *           in case there is an unexpected error
   */
  private boolean importTransformation(Node transnode, RepositoryImportFeedbackInterface feedback) throws KettleException {
    //
    // Load transformation from XML into a directory, possibly created!
    //
    // passing the repository to the TransMeta constructor will result in some expensive server hits
    // for things we are specifically doing in this code, such as transMeta.setRepositoryDirectory(targetDirectory);
    TransMeta transMeta = new TransMeta(transnode, null);
    replaceSharedObjects(transMeta);
    feedback.setLabel(BaseMessages.getString(PKG, "PurRepositoryImporter.ImportTrans.Label", Integer.toString(transformationNumber), transMeta.getName()));

    RepositoryImporter.validateImportedElement(importRules, transMeta);

    // What's the directory path?
    String directoryPath = XMLHandler.getTagValue(transnode, "info", "directory");
    
    if (transDirOverride != null) {
      directoryPath = transDirOverride;
    }
    
    if (directoryPath.startsWith("/")) {
      // remove the leading root, we don't need it.
      directoryPath = directoryPath.substring(1);
    }

    RepositoryDirectoryInterface targetDirectory = getTargetDirectory(directoryPath, transDirOverride, feedback);

    // OK, we loaded the transformation from XML and all went well...
    // See if the transformation already existed!
    ObjectId existingId = rep.getTransformationID(transMeta.getName(), targetDirectory);
    if (existingId!=null && askOverwrite) {
      overwrite = feedback.transOverwritePrompt(transMeta);
      askOverwrite = feedback.isAskingOverwriteConfirmation();
    } else {
      updateDisplay();
    }

    if (existingId==null || overwrite) {
      transMeta.setObjectId(existingId);
      transMeta.setRepositoryDirectory(targetDirectory);
      patchMappingSteps(transMeta);

      try {
        // Keep info on who & when this transformation was created...
        if (transMeta.getCreatedUser() == null || transMeta.getCreatedUser().equals("-")) {
          transMeta.setCreatedDate(new Date());
          if (rep.getUserInfo() != null) {
            transMeta.setCreatedUser(rep.getUserInfo().getLogin());
          } else {
            transMeta.setCreatedUser(null);
          }
        }
        
        rep.saveTrans0(transMeta, versionComment, true, false, false, false, false);
        feedback.addLog(BaseMessages.getString(PKG, "PurRepositoryImporter.TransSaved.Log", Integer.toString(transformationNumber), transMeta.getName()));
        
        if (transMeta.hasRepositoryReferences()) {
          referencingObjects.add(new RepositoryObject(transMeta.getObjectId(), transMeta.getName(), transMeta.getRepositoryDirectory(), null, null, RepositoryObjectType.TRANSFORMATION, null, false));
        }
        
      } catch (Exception e) {
        feedback.addLog(BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorSavingTrans.Log", Integer.toString(transformationNumber), transMeta.getName(), e.toString()));
        feedback.addLog(Const.getStackTracker(e));
      }
    } else {
      feedback.addLog(BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorSavingTrans2.Log", transMeta.getName()));
    }
    return true;
  }

  private boolean importJob(Node jobnode, RepositoryImportFeedbackInterface feedback) throws KettleException {
    // Load the job from the XML node.
    //                
    JobMeta jobMeta = new JobMeta(jobnode, null, false, SpoonFactory.getInstance());
    replaceSharedObjects(jobMeta);
    feedback.setLabel(BaseMessages.getString(PKG, "PurRepositoryImporter.ImportJob.Label", Integer.toString(jobNumber), jobMeta.getName()));

    RepositoryImporter.validateImportedElement(importRules, jobMeta);

    // What's the directory path?
    String directoryPath = Const.NVL(XMLHandler.getTagValue(jobnode, "directory"), Const.FILE_SEPARATOR);

    if (jobDirOverride != null) {
      directoryPath = jobDirOverride;
    }
    
    if (directoryPath.startsWith("/")) {
      // remove the leading root, we don't need it.
      directoryPath = directoryPath.substring(1);
    }

    RepositoryDirectoryInterface targetDirectory = getTargetDirectory(directoryPath, jobDirOverride, feedback);


    // OK, we loaded the job from XML and all went well...
    // See if the job already exists!
    ObjectId existintId = rep.getJobId(jobMeta.getName(), targetDirectory);
    if (existintId != null && askOverwrite) {
      overwrite = feedback.jobOverwritePrompt(jobMeta);
      askOverwrite = feedback.isAskingOverwriteConfirmation();
    } else {
      updateDisplay();
    }

    if (existintId == null || overwrite) {
      jobMeta.setRepositoryDirectory(targetDirectory);
      jobMeta.setObjectId(existintId);
      patchJobEntries(jobMeta);
      rep.saveJob0(jobMeta, versionComment, true, false, false, false, false);
      
      if (jobMeta.hasRepositoryReferences()) {
        referencingObjects.add(new RepositoryObject(jobMeta.getObjectId(), jobMeta.getName(), jobMeta.getRepositoryDirectory(), null, null, RepositoryObjectType.JOB, null, false));
      }

      feedback.addLog(BaseMessages.getString(PKG, "PurRepositoryImporter.JobSaved.Log", Integer.toString(jobNumber), jobMeta.getName()));
    } else {
      feedback.addLog(BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorSavingJob.Log", jobMeta.getName()));
    }
    return true;
  }

  private int transformationNumber = 1;
  public boolean transformationElementRead(String xml, RepositoryImportFeedbackInterface feedback) {
    try {
      Document doc = XMLHandler.loadXMLString(xml);
      Node transformationNode = XMLHandler.getSubNode(doc, RepositoryExportSaxParser.STRING_TRANSFORMATION);
      
      if (!importTransformation(transformationNode, feedback)) {
        return false;
      }
      transformationNumber++;
    } catch (Exception e) {
      // Some unexpected error occurred during transformation import
      // This is usually a problem with a missing plugin or something
      // like that...
      //
      feedback.showError(BaseMessages.getString(PKG, "PurRepositoryImporter.UnexpectedErrorDuringTransformationImport.Title"), 
          BaseMessages.getString(PKG, "PurRepositoryImporter.UnexpectedErrorDuringTransformationImport.Message"), e);

      if (!feedback.askContinueOnErrorQuestion(BaseMessages.getString(PKG, "PurRepositoryImporter.DoYouWantToContinue.Title"), 
          BaseMessages.getString(PKG, "PurRepositoryImporter.DoYouWantToContinue.Message"))) {
        return false;
      }
    }
    return true;
  }

  private int jobNumber = 1;
  public boolean jobElementRead(String xml, RepositoryImportFeedbackInterface feedback) {
    try {
      Document doc = XMLHandler.loadXMLString(xml);
      Node jobNode = XMLHandler.getSubNode(doc, RepositoryExportSaxParser.STRING_JOB);
      if (!importJob(jobNode, feedback)) {
        return false;
      }
      jobNumber++;
    } catch (Exception e) {
      // Some unexpected error occurred during job import
      // This is usually a problem with a missing plugin or something
      // like that...
      //
      showError(BaseMessages.getString(PKG, "PurRepositoryImporter.UnexpectedErrorDuringJobImport.Title"), 
          BaseMessages.getString(PKG, "PurRepositoryImporter.UnexpectedErrorDuringJobImport.Message"), e);
      
      if (!feedback.askContinueOnErrorQuestion(BaseMessages.getString(PKG, "PurRepositoryImporter.DoYouWantToContinue.Title"), 
          BaseMessages.getString(PKG, "PurRepositoryImporter.DoYouWantToContinue.Message"))) {
        return false;
      }
    }
    return true;
  }

  public void fatalXmlErrorEncountered(SAXParseException e) {
    showError(
        BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorInvalidXML.Message"),
        BaseMessages.getString(PKG, "PurRepositoryImporter.ErrorInvalidXML.Title"),
        e
       );
  }

  public boolean askContinueOnErrorQuestion(String title, String message) {
    return continueOnError;
  }
  
  public void beginTask(String message, int nrWorks) {
    addLog(message);
  }

  public void done() {
  }

  public boolean isCanceled() {
    return false;
  }

  public void setTaskName(String taskName) {
    addLog(taskName);
  }

  public void subTask(String message) {
    addLog(message);
  }

  public void worked(int nrWorks) {
  }
  
  public void setImportRules(ImportRules importRules) {
    this.importRules = importRules;
  }
  
  public ImportRules getImportRules() {
    return importRules;
  }

  public boolean isAskingOverwriteConfirmation() {
    return askOverwrite;
  }
  
  
  public String getTransDirOverride() {
    return transDirOverride;
  }

  public void setTransDirOverride(String transDirOverride) {
    this.transDirOverride = transDirOverride;
  }

  public String getJobDirOverride() {
    return jobDirOverride;
  }

  public void setJobDirOverride(String jobDirOverride) {
    this.jobDirOverride = jobDirOverride;
  }
  
  private RepositoryDirectoryInterface getTargetDirectory(String directoryPath, String dirOverride, RepositoryImportFeedbackInterface feedback) throws KettleException {
    RepositoryDirectoryInterface targetDirectory = null;
    if (dirOverride != null) {
      targetDirectory = rep.findDirectory(directoryPath);
      if (targetDirectory == null) {
        feedback.addLog(BaseMessages.getString(PKG, "RepositoryImporter.CreateDir.Log", directoryPath, getRepositoryRoot().toString()));
        targetDirectory = rep.createRepositoryDirectory(getRepositoryRoot(), directoryPath);
      }
    } else {
      targetDirectory = baseDirectory.findDirectory(directoryPath);
      if (targetDirectory == null) {
        feedback.addLog(BaseMessages.getString(PKG, "RepositoryImporter.CreateDir.Log", directoryPath, baseDirectory.toString()));
        targetDirectory = rep.createRepositoryDirectory(baseDirectory, directoryPath);
      }
    }
    return targetDirectory;
  }
  
  private RepositoryDirectoryInterface getRepositoryRoot() throws KettleException {
    if (root == null) {
      root = rep.loadRepositoryDirectoryTree();
    }
    return root;
  }
  
  private void addException(Exception exception) {
     if (this.exceptions == null) {
        this.exceptions = new ArrayList<Exception>();
     }
     exceptions.add(exception);
  }
  
  public List<Exception> getExceptions() {
     return this.exceptions;
  }
  
}
