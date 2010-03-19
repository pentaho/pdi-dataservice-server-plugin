package org.pentaho.di.repository.pur;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepLoaderException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryAttributeInterface;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepErrorMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepPartitioningMeta;

import com.pentaho.repository.pur.data.node.DataNode;
import com.pentaho.repository.pur.data.node.DataNodeRef;

public class TransDelegate extends AbstractDelegate implements ITransformer, ISharedObjectsTransformer {

  private static final String PROP_STEP_ERROR_HANDLING_MIN_PCT_ROWS = "step_error_handling_min_pct_rows";

  private static final String PROP_STEP_ERROR_HANDLING_MAX_PCT_ERRORS = "step_error_handling_max_pct_errors";

  private static final String PROP_STEP_ERROR_HANDLING_MAX_ERRORS = "step_error_handling_max_errors";

  private static final String PROP_STEP_ERROR_HANDLING_CODES_VALUENAME = "step_error_handling_codes_valuename";

  private static final String PROP_STEP_ERROR_HANDLING_FIELDS_VALUENAME = "step_error_handling_fields_valuename";

  private static final String PROP_STEP_ERROR_HANDLING_DESCRIPTIONS_VALUENAME = "step_error_handling_descriptions_valuename";

  private static final String PROP_STEP_ERROR_HANDLING_NR_VALUENAME = "step_error_handling_nr_valuename";

  private static final String PROP_STEP_ERROR_HANDLING_IS_ENABLED = "step_error_handling_is_enabled";

  private static final String PROP_STEP_ERROR_HANDLING_TARGET_STEP = "step_error_handling_target_step";

  private static final String PROP_LOG_SIZE_LIMIT = "LOG_SIZE_LIMIT";

  private static final String PROP_LOG_INTERVAL = "LOG_INTERVAL";

  private static final String PROP_TRANSFORMATION_TYPE = "TRANSFORMATION_TYPE";

  private static final String PROP_STEP_PERFORMANCE_LOG_TABLE = "STEP_PERFORMANCE_LOG_TABLE";

  private static final String PROP_STEP_PERFORMANCE_CAPTURING_DELAY = "STEP_PERFORMANCE_CAPTURING_DELAY";

  private static final String PROP_CAPTURE_STEP_PERFORMANCE = "CAPTURE_STEP_PERFORMANCE";

  private static final String PROP_SHARED_FILE = "SHARED_FILE";

  private static final String PROP_USING_THREAD_PRIORITIES = "USING_THREAD_PRIORITIES";

  private static final String PROP_FEEDBACK_SIZE = "FEEDBACK_SIZE";

  private static final String PROP_FEEDBACK_SHOWN = "FEEDBACK_SHOWN";

  private static final String PROP_UNIQUE_CONNECTIONS = "UNIQUE_CONNECTIONS";

  private static final String PROP_ID_DIRECTORY = "ID_DIRECTORY";

  private static final String PROP_SIZE_ROWSET = "SIZE_ROWSET";

  private static final String PROP_MODIFIED_DATE = "MODIFIED_DATE";

  private static final String PROP_MODIFIED_USER = "MODIFIED_USER";

  private static final String PROP_CREATED_DATE = "CREATED_DATE";

  private static final String PROP_CREATED_USER = "CREATED_USER";

  private static final String PROP_DIFF_MAXDATE = "DIFF_MAXDATE";

  private static final String PROP_OFFSET_MAXDATE = "OFFSET_MAXDATE";

  private static final String PROP_FIELD_NAME_MAXDATE = "FIELD_NAME_MAXDATE";

  private static final String PROP_TABLE_NAME_MAXDATE = "TABLE_NAME_MAXDATE";

  private static final String PROP_ID_DATABASE_MAXDATE = "ID_DATABASE_MAXDATE";

  private static final String PROP_USE_LOGFIELD = "USE_LOGFIELD";

  private static final String PROP_USE_BATCHID = "USE_BATCHID";

  private static final String PROP_TABLE_NAME_LOG = "TABLE_NAME_LOG";

  private static final String PROP_DATABASE_LOG = "DATABASE_LOG";

  private static final String PROP_STEP_REJECTED = "STEP_REJECTED";

  private static final String PROP_STEP_UPDATE = "STEP_UPDATE";

  private static final String PROP_STEP_OUTPUT = "STEP_OUTPUT";

  private static final String PROP_STEP_INPUT = "STEP_INPUT";

  private static final String PROP_STEP_WRITE = "STEP_WRITE";

  private static final String PROP_STEP_READ = "STEP_READ";

  private static final String PROP_TRANS_STATUS = "TRANS_STATUS";

  private static final String PROP_TRANS_VERSION = "TRANS_VERSION";

  private static final String PROP_EXTENDED_DESCRIPTION = "EXTENDED_DESCRIPTION";

  private static final String PROP_NR_PARAMETERS = "NR_PARAMETERS";

  private static final String NODE_PARAMETERS = "parameters";

  private static final String PROP_NR_HOPS = "NR_HOPS";

  private static final String PROP_NR_NOTES = "NR_NOTES";

  private static final String NODE_NOTES = "notes";

  private static final String NODE_HOPS = "hops";

  private static final String PROP_STEP_ERROR_HANDLING_SOURCE_STEP = "step_error_handling_source_step";

  private static final String NODE_PARTITIONER_CUSTOM = "partitionerCustom";

  private static final String PROP_PARTITIONING_SCHEMA = "PARTITIONING_SCHEMA";

  private static final String PROP_PARTITIONING_METHOD = "PARTITIONING_METHOD";

  private static final String PROP_CLUSTER_SCHEMA = "cluster_schema";

  private static final String NODE_STEP_CUSTOM = "custom";

  private static Class<?> PKG = TransDelegate.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private static final String PROP_STEP_DISTRIBUTE = "STEP_DISTRIBUTE";

  private static final String PROP_STEP_GUI_DRAW = "STEP_GUI_DRAW";

  private static final String PROP_STEP_GUI_LOCATION_Y = "STEP_GUI_LOCATION_Y";

  private static final String PROP_STEP_GUI_LOCATION_X = "STEP_GUI_LOCATION_X";

  private static final String PROP_STEP_COPIES = "STEP_COPIES";

  private static final String PROP_STEP_TYPE = "STEP_TYPE";

  private static final String NODE_TRANS = "transformation";

  private static final String EXT_STEP = ".kst";

  private static final String NODE_STEPS = "steps";

  private static final String PROP_XML = "XML";

  private static final String NOTE_PREFIX = "__NOTE__#";

  private static final String TRANS_HOP_FROM = "TRANS_HOP_FROM";

  private static final String TRANS_HOP_TO = "TRANS_HOP_TO";

  private static final String TRANS_HOP_ENABLED = "TRANS_HOP_ENABLED";

  private static final String TRANS_HOP_PREFIX = "__TRANS_HOP__#";

  private static final String TRANS_PARAM_PREFIX = "__TRANS_PARAM__#";

  private static final String PARAM_KEY = "PARAM_KEY";

  private static final String PARAM_DESC = "PARAM_DESC";

  private static final String PARAM_DEFAULT = "PARAM_DEFAULT";

  private Repository repo;

  public TransDelegate(final Repository repo) {
    super();
    this.repo = repo;
  }

  public RepositoryElementInterface dataNodeToElement(final DataNode rootNode) throws KettleException {
    TransMeta transMeta = new TransMeta();
    dataNodeToElement(rootNode, transMeta);
    return transMeta;
  }

  public void dataNodeToElement(final DataNode rootNode, final RepositoryElementInterface element)
      throws KettleException {
    TransMeta transMeta = (TransMeta) element;

    transMeta.setName(getString(rootNode, PROP_NAME));
    transMeta.setDescription(getString(rootNode, PROP_DESCRIPTION));

    // read the steps...
    //
    DataNode stepsNode = rootNode.getNode(NODE_STEPS);
    for (DataNode stepNode : stepsNode.getNodes()) {

      StepMeta stepMeta = new StepMeta(new StringObjectId(stepNode.getId().toString()));
      stepMeta.setParentTransMeta(transMeta); // for tracing, retain hierarchy

      // Read the basics
      //
      stepMeta.setName(getString(stepNode, PROP_NAME));
      if (stepNode.hasProperty(PROP_DESCRIPTION)) {
        stepMeta.setDescription(getString(stepNode, PROP_DESCRIPTION));
      }
      stepMeta.setDistributes(stepNode.getProperty(PROP_STEP_DISTRIBUTE).getBoolean());
      stepMeta.setDraw(stepNode.getProperty(PROP_STEP_GUI_DRAW).getBoolean());
      stepMeta.setCopies((int) stepNode.getProperty(PROP_STEP_COPIES).getLong());

      int x = (int) stepNode.getProperty(PROP_STEP_GUI_LOCATION_X).getLong();
      int y = (int) stepNode.getProperty(PROP_STEP_GUI_LOCATION_Y).getLong();
      stepMeta.setLocation(x, y);

      String stepType = getString(stepNode, PROP_STEP_TYPE);

      // Create a new StepMetaInterface object...
      //
      PluginRegistry registry = PluginRegistry.getInstance();
      PluginInterface stepPlugin = registry.findPluginWithId(StepPluginType.class, stepType);
      
      StepMetaInterface stepMetaInterface = null;
      if (stepPlugin != null) {
        stepMetaInterface = (StepMetaInterface)registry.loadClass(stepPlugin);
        stepType = stepPlugin.getIds()[0]; // revert to the default in case we loaded an alternate version
      } else {
        throw new KettleStepLoaderException(BaseMessages.getString(PKG, "StepMeta.Exception.UnableToLoadClass", stepType)); //$NON-NLS-1$
      }

      stepMeta.setStepID(stepType);

      // Read the metadata from the repository too...
      //
      RepositoryProxy proxy = new RepositoryProxy(stepNode.getNode(NODE_STEP_CUSTOM));
      stepMetaInterface.readRep(proxy, null, transMeta.getDatabases(), transMeta.getCounters());
      stepMeta.setStepMetaInterface(stepMetaInterface);

      // Get the partitioning as well...
      StepPartitioningMeta stepPartitioningMeta = new StepPartitioningMeta();
      if (stepNode.hasProperty(PROP_PARTITIONING_SCHEMA)) {
        String partSchemaId = stepNode.getProperty(PROP_PARTITIONING_SCHEMA).getRef().getId().toString();
        String schemaName = repo.loadPartitionSchema(new StringObjectId(partSchemaId), null).getName();

        stepPartitioningMeta.setPartitionSchemaName(schemaName);
        String methodCode = getString(stepNode, PROP_PARTITIONING_METHOD);
        stepPartitioningMeta.setMethod(StepPartitioningMeta.getMethod(methodCode));
        if (stepPartitioningMeta.getPartitioner() != null) {
          proxy = new RepositoryProxy(stepNode.getNode(NODE_PARTITIONER_CUSTOM));
          stepPartitioningMeta.getPartitioner().loadRep(proxy, null);
        }
        stepPartitioningMeta.hasChanged(true);
      }
      stepMeta.setStepPartitioningMeta(stepPartitioningMeta);

      stepMeta.getStepPartitioningMeta().setPartitionSchemaAfterLoading(transMeta.getPartitionSchemas());
      // Get the cluster schema name
      stepMeta.setClusterSchemaName(getString(stepNode, PROP_CLUSTER_SCHEMA));

      transMeta.addStep(stepMeta);

      // Also load the step error handling metadata
      //
      if (stepNode.hasProperty(PROP_STEP_ERROR_HANDLING_SOURCE_STEP)) {
        StepErrorMeta meta = new StepErrorMeta(transMeta, stepMeta);
        meta.setTargetStep(StepMeta.findStep(transMeta.getSteps(), stepNode.getProperty(
            PROP_STEP_ERROR_HANDLING_TARGET_STEP).getString()));
        meta.setEnabled(stepNode.getProperty(PROP_STEP_ERROR_HANDLING_IS_ENABLED).getBoolean());
        meta.setNrErrorsValuename(getString(stepNode, PROP_STEP_ERROR_HANDLING_NR_VALUENAME));
        meta.setErrorDescriptionsValuename(stepNode.getProperty(PROP_STEP_ERROR_HANDLING_DESCRIPTIONS_VALUENAME)
            .getString());
        meta.setErrorFieldsValuename(getString(stepNode, PROP_STEP_ERROR_HANDLING_FIELDS_VALUENAME));
        meta.setErrorCodesValuename(getString(stepNode, PROP_STEP_ERROR_HANDLING_CODES_VALUENAME));
        meta.setMaxErrors(stepNode.getProperty(PROP_STEP_ERROR_HANDLING_MAX_ERRORS).getLong());
        meta.setMaxPercentErrors((int) stepNode.getProperty(PROP_STEP_ERROR_HANDLING_MAX_PCT_ERRORS).getLong());
        meta.setMinPercentRows(stepNode.getProperty(PROP_STEP_ERROR_HANDLING_MIN_PCT_ROWS).getLong());
        meta.getSourceStep().setStepErrorMeta(meta); // a bit of a trick, I know.                        
      }
    }
    
    // Have all StreamValueLookups, etc. reference the correct source steps...
    //
    for (int i = 0; i < transMeta.nrSteps(); i++)
    {
        StepMeta stepMeta = transMeta.getStep(i);
        StepMetaInterface sii = stepMeta.getStepMetaInterface();
        if (sii != null) sii.searchInfoAndTargetSteps(transMeta.getSteps());
    }
    
    // Read the notes...
    //
    DataNode notesNode = rootNode.getNode(NODE_NOTES);
    int nrNotes = (int) notesNode.getProperty(PROP_NR_NOTES).getLong();
    for (DataNode noteNode : notesNode.getNodes()) {
      String xml = getString(noteNode, PROP_XML);
      transMeta.addNote(new NotePadMeta(XMLHandler.getSubNode(XMLHandler.loadXMLString(xml), NotePadMeta.XML_TAG)));
    }
    if (transMeta.nrNotes() != nrNotes) {
      throw new KettleException("The number of notes read [" + transMeta.nrNotes()
          + "] was not the number we expected [" + nrNotes + "]");
    }

    // Read the hops...
    //
    DataNode hopsNode = rootNode.getNode(NODE_HOPS);
    int nrHops = (int) hopsNode.getProperty(PROP_NR_HOPS).getLong();
    for (DataNode hopNode : hopsNode.getNodes()) {
      String stepFromName = getString(hopNode, TRANS_HOP_FROM);
      String stepToName = getString(hopNode, TRANS_HOP_TO);
      boolean enabled = true;
      if (hopNode.hasProperty(TRANS_HOP_ENABLED)) {
        enabled = hopNode.getProperty(TRANS_HOP_ENABLED).getBoolean();
      }

      System.out.println("Read Hop from " + stepFromName + " --> " + stepToName); //$NON-NLS-1$ //$NON-NLS-2$

      StepMeta stepFrom = StepMeta.findStep(transMeta.getSteps(), stepFromName);
      StepMeta stepTo = StepMeta.findStep(transMeta.getSteps(), stepToName);

      transMeta.addTransHop(new TransHopMeta(stepFrom, stepTo, enabled));

    }
    if (transMeta.nrTransHops() != nrHops) {
      throw new KettleException("The number of hops read [" + transMeta.nrTransHops()
          + "] was not the number we expected [" + nrHops + "]");
    }

    // Load the details at the end, to make sure we reference the databases correctly, etc.
    //
    loadTransformationDetails(rootNode, transMeta);

    transMeta.eraseParameters();

    DataNode paramsNode = rootNode.getNode(NODE_PARAMETERS);

    int count = (int) paramsNode.getProperty(PROP_NR_PARAMETERS).getLong();
    for (int idx = 0; idx < count; idx++) {
      DataNode paramNode = paramsNode.getNode(TRANS_PARAM_PREFIX + idx);
      String key = getString(paramNode, PARAM_KEY);
      String def = getString(paramNode, PARAM_DEFAULT);
      String desc = getString(paramNode, PARAM_DESC);
      transMeta.addParameterDefinition(key, def, desc);
    }
  }

  protected void loadTransformationDetails(final DataNode rootNode, final TransMeta transMeta) throws KettleException {
    transMeta.setExtendedDescription(getString(rootNode, PROP_EXTENDED_DESCRIPTION));
    transMeta.setTransversion(getString(rootNode, PROP_TRANS_VERSION));
    transMeta.setTransstatus((int) rootNode.getProperty(PROP_TRANS_STATUS).getLong());

    if (rootNode.hasProperty(PROP_STEP_READ)) {
      transMeta.getTransLogTable().setStepRead(
          StepMeta.findStep(transMeta.getSteps(), getString(rootNode, PROP_STEP_READ)));
    }
    if (rootNode.hasProperty(PROP_STEP_WRITE)) {
      transMeta.getTransLogTable().setStepWritten(
          StepMeta.findStep(transMeta.getSteps(), getString(rootNode, PROP_STEP_WRITE)));
    }
    if (rootNode.hasProperty(PROP_STEP_INPUT)) {
      transMeta.getTransLogTable().setStepInput(
          StepMeta.findStep(transMeta.getSteps(), getString(rootNode, PROP_STEP_INPUT)));
    }
    if (rootNode.hasProperty(PROP_STEP_OUTPUT)) {
      transMeta.getTransLogTable().setStepOutput(
          StepMeta.findStep(transMeta.getSteps(), getString(rootNode, PROP_STEP_OUTPUT)));
    }
    if (rootNode.hasProperty(PROP_STEP_UPDATE)) {
      transMeta.getTransLogTable().setStepUpdate(
          StepMeta.findStep(transMeta.getSteps(), getString(rootNode, PROP_STEP_UPDATE)));
    }
    if (rootNode.hasProperty(PROP_STEP_REJECTED)) {
      transMeta.getTransLogTable().setStepRejected(
          StepMeta.findStep(transMeta.getSteps(), getString(rootNode, PROP_STEP_REJECTED)));
    }

    if (rootNode.hasProperty(PROP_DATABASE_LOG)) {
      String id = rootNode.getProperty(PROP_DATABASE_LOG).getRef().getId().toString();
      DatabaseMeta conn = DatabaseMeta.findDatabase(transMeta.getDatabases(), new StringObjectId(id));
      transMeta.getTransLogTable().setConnectionName(conn.getName());
    }
    transMeta.getTransLogTable().setTableName(getString(rootNode, PROP_TABLE_NAME_LOG));
    transMeta.getTransLogTable().setBatchIdUsed(rootNode.getProperty(PROP_USE_BATCHID).getBoolean());
    transMeta.getTransLogTable().setLogFieldUsed(rootNode.getProperty(PROP_USE_LOGFIELD).getBoolean());

    if (rootNode.hasProperty(PROP_ID_DATABASE_MAXDATE)) {
      String id = rootNode.getProperty(PROP_ID_DATABASE_MAXDATE).getRef().getId().toString();
      transMeta.setMaxDateConnection(DatabaseMeta.findDatabase(transMeta.getDatabases(), new StringObjectId(id)));
    }
    transMeta.setMaxDateTable(getString(rootNode, PROP_TABLE_NAME_MAXDATE));
    transMeta.setMaxDateField(getString(rootNode, PROP_FIELD_NAME_MAXDATE));
    transMeta.setMaxDateOffset(rootNode.getProperty(PROP_OFFSET_MAXDATE).getDouble());
    transMeta.setMaxDateDifference(rootNode.getProperty(PROP_DIFF_MAXDATE).getDouble());

    transMeta.setCreatedUser(getString(rootNode, PROP_CREATED_USER));
    transMeta.setCreatedDate(getDate(rootNode, PROP_CREATED_DATE));

    transMeta.setModifiedUser(getString(rootNode, PROP_MODIFIED_USER));
    transMeta.setModifiedDate(getDate(rootNode, PROP_MODIFIED_DATE));

    // Optional:
    transMeta.setSizeRowset(Const.ROWS_IN_ROWSET);
    long val_size_rowset = rootNode.getProperty(PROP_SIZE_ROWSET).getLong();
    if (val_size_rowset > 0) {
      transMeta.setSizeRowset((int) val_size_rowset);
    }

    if (rootNode.hasProperty(PROP_ID_DIRECTORY)) {
      String id_directory = getString(rootNode, PROP_ID_DIRECTORY);
      if (log.isDetailed())
        log.logDetailed(toString(), PROP_ID_DIRECTORY + "=" + id_directory); //$NON-NLS-1$
      // Set right directory...
      transMeta.setRepositoryDirectory(repo.loadRepositoryDirectoryTree().findDirectory(
          new StringObjectId(id_directory))); // always reload the folder structure
    }

    transMeta.setUsingUniqueConnections(rootNode.getProperty(PROP_UNIQUE_CONNECTIONS).getBoolean());
    boolean feedbackShown = true;
    if (rootNode.hasProperty(PROP_FEEDBACK_SHOWN)) {
      feedbackShown = rootNode.getProperty(PROP_FEEDBACK_SHOWN).getBoolean();
    }
    transMeta.setFeedbackShown(feedbackShown);
    transMeta.setFeedbackSize((int) rootNode.getProperty(PROP_FEEDBACK_SIZE).getLong());
    boolean usingThreadPriorityManagement = true;
    if (rootNode.hasProperty(PROP_USING_THREAD_PRIORITIES)) {
      usingThreadPriorityManagement = rootNode.getProperty(PROP_USING_THREAD_PRIORITIES).getBoolean();
    }
    transMeta.setUsingThreadPriorityManagment(usingThreadPriorityManagement);
    transMeta.setSharedObjectsFile(getString(rootNode, PROP_SHARED_FILE));

    // Performance monitoring for steps...
    //
    boolean capturingStepPerformanceSnapShots = true;
    if (rootNode.hasProperty(PROP_CAPTURE_STEP_PERFORMANCE)) {
      capturingStepPerformanceSnapShots = rootNode.getProperty(PROP_CAPTURE_STEP_PERFORMANCE).getBoolean();
    }
    transMeta.setCapturingStepPerformanceSnapShots(capturingStepPerformanceSnapShots);
    transMeta.setStepPerformanceCapturingDelay(rootNode.getProperty(PROP_STEP_PERFORMANCE_CAPTURING_DELAY).getLong());
    transMeta.getPerformanceLogTable().setTableName(getString(rootNode, PROP_STEP_PERFORMANCE_LOG_TABLE));
    transMeta.getTransLogTable().setLogSizeLimit(getString(rootNode, PROP_LOG_SIZE_LIMIT));
    //transMeta.setStepPerformanceLogTable( repository.getPropertyString(rootNode, "STEP_PERFORMANCE_LOG_TABLE") );
    //transMeta.setLogSizeLimit( repository.getPropertyString(rootNode, "LOG_SIZE_LIMIT") );

    
    // Load the logging tables too..
    //
	RepositoryAttributeInterface attributeInterface = new PurRepositoryAttribute(rootNode, transMeta.getDatabases());
    transMeta.getTransLogTable().loadFromRepository(attributeInterface);
    transMeta.getStepLogTable().loadFromRepository(attributeInterface);
    transMeta.getPerformanceLogTable().loadFromRepository(attributeInterface);
    transMeta.getChannelLogTable().loadFromRepository(attributeInterface);
  }

  public DataNode elementToDataNode(final RepositoryElementInterface element) throws KettleException {
    TransMeta transMeta = (TransMeta) element;

    DataNode rootNode = new DataNode(NODE_TRANS);

    rootNode.setProperty(PROP_NAME, transMeta.getName());
    rootNode.setProperty(PROP_DESCRIPTION, Const.NVL(transMeta.getDescription(), "")); //$NON-NLS-1$

    DataNode stepsNode = rootNode.addNode(NODE_STEPS);

    // Also save all the steps in the transformation!
    //
    int stepNr = 0;
    for (StepMeta step : transMeta.getSteps()) {
      stepNr++;
      DataNode stepNode = stepsNode.addNode(sanitizeNodeName(step.getName()) + "_" + stepNr + EXT_STEP); //$NON-NLS-1$

      // Store the main data
      //
      stepNode.setProperty(PROP_NAME, step.getName());
      stepNode.setProperty(PROP_DESCRIPTION, step.getDescription());
      stepNode.setProperty(PROP_STEP_TYPE, step.getStepID());
      stepNode.setProperty(PROP_STEP_DISTRIBUTE, step.isDistributes());
      stepNode.setProperty(PROP_STEP_COPIES, step.getCopies());
      stepNode.setProperty(PROP_STEP_GUI_LOCATION_X, step.getLocation().x);
      stepNode.setProperty(PROP_STEP_GUI_LOCATION_Y, step.getLocation().y);
      stepNode.setProperty(PROP_STEP_GUI_DRAW, step.isDrawn());

      // Save the step metadata using the repository save method, NOT XML
      // That is because we want to keep the links to databases, conditions, etc by ID, not name.
      //
      StepMetaInterface stepMetaInterface = step.getStepMetaInterface();
      DataNode stepCustomNode = new DataNode(NODE_STEP_CUSTOM);
      Repository proxy = new RepositoryProxy(stepCustomNode);
      stepMetaInterface.saveRep(proxy, null, null);
      stepNode.addNode(stepCustomNode);

      // Save the partitioning information by reference as well...
      //
      StepPartitioningMeta partitioningMeta = step.getStepPartitioningMeta();
      if (partitioningMeta != null && partitioningMeta.getPartitionSchema() != null && partitioningMeta.isPartitioned()) {
        DataNodeRef ref = new DataNodeRef(partitioningMeta.getPartitionSchema().getObjectId().getId());
        stepNode.setProperty(PROP_PARTITIONING_SCHEMA, ref);
        stepNode.setProperty(PROP_PARTITIONING_METHOD, partitioningMeta.getMethodCode()); // method of partitioning
        if (partitioningMeta.getPartitioner() != null) {
          DataNode partitionerCustomNode = new DataNode(NODE_PARTITIONER_CUSTOM);
          proxy = new RepositoryProxy(partitionerCustomNode);
          partitioningMeta.getPartitioner().saveRep(proxy, null, null);
          stepNode.addNode(partitionerCustomNode);
        }
      }

      // Save the clustering information as well...
      //
      stepNode.setProperty(PROP_CLUSTER_SCHEMA, step.getClusterSchema() == null ? "" : step.getClusterSchema() //$NON-NLS-1$
          .getName());

      // Save the error hop metadata
      //
      StepErrorMeta stepErrorMeta = step.getStepErrorMeta();
      if (stepErrorMeta != null) {
        stepNode.setProperty(PROP_STEP_ERROR_HANDLING_SOURCE_STEP,
            stepErrorMeta.getSourceStep() != null ? stepErrorMeta.getSourceStep().getName() : ""); //$NON-NLS-1$
        stepNode.setProperty(PROP_STEP_ERROR_HANDLING_TARGET_STEP,
            stepErrorMeta.getTargetStep() != null ? stepErrorMeta.getTargetStep().getName() : ""); //$NON-NLS-1$
        stepNode.setProperty(PROP_STEP_ERROR_HANDLING_IS_ENABLED, stepErrorMeta.isEnabled());
        stepNode.setProperty(PROP_STEP_ERROR_HANDLING_NR_VALUENAME, stepErrorMeta.getNrErrorsValuename());
        stepNode.setProperty(PROP_STEP_ERROR_HANDLING_DESCRIPTIONS_VALUENAME, stepErrorMeta
            .getErrorDescriptionsValuename());
        stepNode.setProperty(PROP_STEP_ERROR_HANDLING_FIELDS_VALUENAME, stepErrorMeta.getErrorFieldsValuename());
        stepNode.setProperty(PROP_STEP_ERROR_HANDLING_CODES_VALUENAME, stepErrorMeta.getErrorCodesValuename());
        stepNode.setProperty(PROP_STEP_ERROR_HANDLING_MAX_ERRORS, stepErrorMeta.getMaxErrors());
        stepNode.setProperty(PROP_STEP_ERROR_HANDLING_MAX_PCT_ERRORS, stepErrorMeta.getMaxPercentErrors());
        stepNode.setProperty(PROP_STEP_ERROR_HANDLING_MIN_PCT_ROWS, stepErrorMeta.getMinPercentRows());
      }

    }

    // Save the notes
    //
    DataNode notesNode = rootNode.addNode(NODE_NOTES);
    notesNode.setProperty(PROP_NR_NOTES, transMeta.nrNotes());
    for (int i = 0; i < transMeta.nrNotes(); i++) {
      NotePadMeta note = transMeta.getNote(i);
      DataNode noteNode = notesNode.addNode(NOTE_PREFIX + i);

      noteNode.setProperty(PROP_XML, note.getXML());
    }

    // Finally, save the hops
    //
    DataNode hopsNode = rootNode.addNode(NODE_HOPS);
    hopsNode.setProperty(PROP_NR_HOPS, transMeta.nrTransHops());
    for (int i = 0; i < transMeta.nrTransHops(); i++) {
      TransHopMeta hop = transMeta.getTransHop(i);
      DataNode hopNode = hopsNode.addNode(TRANS_HOP_PREFIX + i);
      hopNode.setProperty(TRANS_HOP_FROM, hop.getFromStep().getName());
      hopNode.setProperty(TRANS_HOP_TO, hop.getToStep().getName());
      hopNode.setProperty(TRANS_HOP_ENABLED, hop.isEnabled());
    }

    // Parameters
    //
    String[] paramKeys = transMeta.listParameters();
    DataNode paramsNode = rootNode.addNode(NODE_PARAMETERS);
    paramsNode.setProperty(PROP_NR_PARAMETERS, paramKeys == null ? 0 : paramKeys.length);

    for (int idx = 0; idx < paramKeys.length; idx++) {
      DataNode paramNode = paramsNode.addNode(TRANS_PARAM_PREFIX + idx);
      String key = paramKeys[idx];
      String description = transMeta.getParameterDescription(paramKeys[idx]);
      String defaultValue = transMeta.getParameterDefault(paramKeys[idx]);

      paramNode.setProperty(PARAM_KEY, key != null ? key : ""); //$NON-NLS-1$
      paramNode.setProperty(PARAM_DEFAULT, defaultValue != null ? defaultValue : ""); //$NON-NLS-1$
      paramNode.setProperty(PARAM_DESC, description != null ? description : ""); //$NON-NLS-1$
    }

    // Let's not forget to save the details of the transformation itself.
    // This includes logging information, parameters, etc.
    //
    saveTransformationDetails(rootNode, transMeta);

    return rootNode;
  }

  private void saveTransformationDetails(final DataNode rootNode, final TransMeta transMeta) throws KettleException {

    rootNode.setProperty(PROP_EXTENDED_DESCRIPTION, transMeta.getExtendedDescription());
    rootNode.setProperty(PROP_TRANS_VERSION, transMeta.getTransversion());
    rootNode.setProperty(PROP_TRANS_STATUS, transMeta.getTransstatus() < 0 ? -1L : transMeta.getTransstatus());

    rootNode.setProperty(PROP_STEP_READ, transMeta.getTransLogTable().getStepnameRead());
    rootNode.setProperty(PROP_STEP_WRITE, transMeta.getTransLogTable().getStepnameWritten());
    rootNode.setProperty(PROP_STEP_INPUT, transMeta.getTransLogTable().getStepnameInput());
    rootNode.setProperty(PROP_STEP_OUTPUT, transMeta.getTransLogTable().getStepnameOutput());
    rootNode.setProperty(PROP_STEP_UPDATE, transMeta.getTransLogTable().getStepnameUpdated());
    rootNode.setProperty(PROP_STEP_REJECTED, transMeta.getTransLogTable().getStepnameRejected());

    if (transMeta.getTransLogTable().getDatabaseMeta() != null) {
      DataNodeRef ref = new DataNodeRef(transMeta.getTransLogTable().getDatabaseMeta().getObjectId().getId());
      rootNode.setProperty(PROP_DATABASE_LOG, ref);
    }

    rootNode.setProperty(PROP_TABLE_NAME_LOG, transMeta.getTransLogTable().getTableName());

    rootNode.setProperty(PROP_USE_BATCHID, Boolean.valueOf(transMeta.getTransLogTable().isBatchIdUsed()));
    rootNode.setProperty(PROP_USE_LOGFIELD, Boolean.valueOf(transMeta.getTransLogTable().isLogFieldUsed()));

    if (transMeta.getMaxDateConnection() != null) {
      DataNodeRef ref = new DataNodeRef(transMeta.getMaxDateConnection().getObjectId().getId());
      rootNode.setProperty(PROP_ID_DATABASE_MAXDATE, ref);
    }

    rootNode.setProperty(PROP_TABLE_NAME_MAXDATE, transMeta.getMaxDateTable());
    rootNode.setProperty(PROP_FIELD_NAME_MAXDATE, transMeta.getMaxDateField());
    rootNode.setProperty(PROP_OFFSET_MAXDATE, new Double(transMeta.getMaxDateOffset()));
    rootNode.setProperty(PROP_DIFF_MAXDATE, new Double(transMeta.getMaxDateDifference()));

    rootNode.setProperty(PROP_CREATED_USER, transMeta.getCreatedUser());
    rootNode.setProperty(PROP_CREATED_DATE, transMeta.getCreatedDate());

    rootNode.setProperty(PROP_MODIFIED_USER, transMeta.getModifiedUser());
    rootNode.setProperty(PROP_MODIFIED_DATE, transMeta.getModifiedDate());

    rootNode.setProperty(PROP_SIZE_ROWSET, transMeta.getSizeRowset());

    rootNode.setProperty(PROP_UNIQUE_CONNECTIONS, transMeta.isUsingUniqueConnections());
    rootNode.setProperty(PROP_FEEDBACK_SHOWN, transMeta.isFeedbackShown());
    rootNode.setProperty(PROP_FEEDBACK_SIZE, transMeta.getFeedbackSize());
    rootNode.setProperty(PROP_USING_THREAD_PRIORITIES, transMeta.isUsingThreadPriorityManagment());
    rootNode.setProperty(PROP_SHARED_FILE, transMeta.getSharedObjectsFile());

    rootNode.setProperty(PROP_CAPTURE_STEP_PERFORMANCE, transMeta.isCapturingStepPerformanceSnapShots());
    rootNode.setProperty(PROP_STEP_PERFORMANCE_CAPTURING_DELAY, transMeta.getStepPerformanceCapturingDelay());
    rootNode.setProperty(PROP_STEP_PERFORMANCE_LOG_TABLE, transMeta.getPerformanceLogTable().getTableName());

    rootNode.setProperty(PROP_LOG_SIZE_LIMIT, transMeta.getTransLogTable().getLogSizeLimit());
    rootNode.setProperty(PROP_LOG_INTERVAL, transMeta.getTransLogTable().getLogInterval());

    rootNode.setProperty(PROP_TRANSFORMATION_TYPE, transMeta.getTransformationType().getCode());
    
    // Save the logging tables too..
    //
	RepositoryAttributeInterface attributeInterface = new PurRepositoryAttribute(rootNode, transMeta.getDatabases());
    transMeta.getTransLogTable().saveToRepository(attributeInterface);
    transMeta.getStepLogTable().saveToRepository(attributeInterface);
    transMeta.getPerformanceLogTable().saveToRepository(attributeInterface);
    transMeta.getChannelLogTable().saveToRepository(attributeInterface);
  }

  public SharedObjects loadSharedObjects(final RepositoryElementInterface element) throws KettleException {
    TransMeta transMeta = (TransMeta) element;
    transMeta.setSharedObjects(transMeta.readSharedObjects());

    // Repository objects take priority so let's overwrite them...
    //
    readDatabases(transMeta, true);
    readPartitionSchemas(transMeta, true);
    readSlaves(transMeta, true);
    readClusters(transMeta, true);

    return transMeta.getSharedObjects();
  }

  /**
   * Read all the databases from the repository, insert into the TransMeta object, overwriting optionally
   * 
   * @param TransMeta The transformation to load into.
   * @param overWriteShared if an object with the same name exists, overwrite
   * @throws KettleException 
   */
  protected void readDatabases(TransMeta transMeta, boolean overWriteShared) throws KettleException {
    try {
      ObjectId dbids[] = repo.getDatabaseIDs(false);
      for (int i = 0; i < dbids.length; i++) {
        DatabaseMeta databaseMeta = repo.loadDatabaseMeta(dbids[i], null); // Load the last version
        databaseMeta.shareVariablesWith(transMeta);

        DatabaseMeta check = transMeta.findDatabase(databaseMeta.getName()); // Check if there already is one in the transformation
        if (check == null || overWriteShared) // We only add, never overwrite database connections. 
        {
          if (databaseMeta.getName() != null) {
            transMeta.addOrReplaceDatabase(databaseMeta);
            if (!overWriteShared)
              databaseMeta.setChanged(false);
          }
        }
      }
      transMeta.clearChangedDatabases();
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(PKG,
          "JCRRepository.Exception.UnableToReadDatabasesFromRepository"), e); //$NON-NLS-1$
    }
  }

  /**
   * Read the clusters in the repository and add them to this transformation if they are not yet present.
   * @param TransMeta The transformation to load into.
   * @param overWriteShared if an object with the same name exists, overwrite
   * @throws KettleException 
   */
  protected void readClusters(TransMeta transMeta, boolean overWriteShared) throws KettleException {
    try {
      ObjectId dbids[] = repo.getClusterIDs(false);
      for (int i = 0; i < dbids.length; i++) {
        ClusterSchema clusterSchema = repo.loadClusterSchema(dbids[i], transMeta.getSlaveServers(), null); // Read the last version
        clusterSchema.shareVariablesWith(transMeta);
        ClusterSchema check = transMeta.findClusterSchema(clusterSchema.getName()); // Check if there already is one in the transformation
        if (check == null || overWriteShared) {
          if (!Const.isEmpty(clusterSchema.getName())) {
            transMeta.addOrReplaceClusterSchema(clusterSchema);
            if (!overWriteShared)
              clusterSchema.setChanged(false);
          }
        }
      }
    } catch (KettleDatabaseException dbe) {
      throw new KettleException(
          BaseMessages.getString(PKG, "JCRRepository.Log.UnableToReadClustersFromRepository"), dbe); //$NON-NLS-1$
    }
  }

  /**
   * Read the partitions in the repository and add them to this transformation if they are not yet present.
   * @param TransMeta The transformation to load into.
   * @param overWriteShared if an object with the same name exists, overwrite
   * @throws KettleException 
   */
  protected void readPartitionSchemas(TransMeta transMeta, boolean overWriteShared) throws KettleException {
    try {
      ObjectId dbids[] = repo.getPartitionSchemaIDs(false);
      for (int i = 0; i < dbids.length; i++) {
        PartitionSchema partitionSchema = repo.loadPartitionSchema(dbids[i], null); // Load the last version
        PartitionSchema check = transMeta.findPartitionSchema(partitionSchema.getName()); // Check if there already is one in the transformation
        if (check == null || overWriteShared) {
          if (!Const.isEmpty(partitionSchema.getName())) {
            transMeta.addOrReplacePartitionSchema(partitionSchema);
            if (!overWriteShared)
              partitionSchema.setChanged(false);
          }
        }
      }
    } catch (KettleException dbe) {
      throw new KettleException(BaseMessages.getString(PKG,
          "JCRRepository.Log.UnableToReadPartitionSchemaFromRepository"), dbe); //$NON-NLS-1$
    }
  }

  /**
   * Read the slave servers in the repository and add them to this transformation if they are not yet present.
   * @param TransMeta The transformation to load into.
   * @param overWriteShared if an object with the same name exists, overwrite
   * @throws KettleException 
   */
  protected void readSlaves(TransMeta transMeta, boolean overWriteShared) throws KettleException {
    try {
      ObjectId dbids[] = repo.getSlaveIDs(false);
      for (int i = 0; i < dbids.length; i++) {
        SlaveServer slaveServer = repo.loadSlaveServer(dbids[i], null); // load the last version
        slaveServer.shareVariablesWith(transMeta);
        SlaveServer check = transMeta.findSlaveServer(slaveServer.getName()); // Check if there already is one in the transformation
        if (check == null || overWriteShared) {
          if (!Const.isEmpty(slaveServer.getName())) {
            transMeta.addOrReplaceSlaveServer(slaveServer);
            if (!overWriteShared)
              slaveServer.setChanged(false);
          }
        }
      }
    } catch (KettleDatabaseException dbe) {
      throw new KettleException(
          BaseMessages.getString(PKG, "JCRRepository.Log.UnableToReadSlaveServersFromRepository"), dbe); //$NON-NLS-1$
    }
  }

  public void saveSharedObjects(final RepositoryElementInterface element, final String versionComment)
      throws KettleException {
    TransMeta transMeta = (TransMeta) element;
    // First store the databases and other depending objects in the transformation.
    //

    // Only store if the database has actually changed or doesn't have an object ID (imported)
    //
    for (DatabaseMeta databaseMeta : transMeta.getDatabases()) {
      if (databaseMeta.hasChanged() || databaseMeta.getObjectId() == null) {

        // Only save the connection if it's actually used in the transformation...
        //
        if (transMeta.isDatabaseConnectionUsed(databaseMeta)) {
          repo.save(databaseMeta, versionComment, null);
        }
      }
    }

    // Store the slave servers...
    //
    for (SlaveServer slaveServer : transMeta.getSlaveServers()) {
      if (slaveServer.hasChanged() || slaveServer.getObjectId() == null) {
        if (transMeta.isUsingSlaveServer(slaveServer)) {
          repo.save(slaveServer, versionComment, null);
        }
      }
    }

    // Store the cluster schemas
    //
    for (ClusterSchema clusterSchema : transMeta.getClusterSchemas()) {
      if (clusterSchema.hasChanged() || clusterSchema.getObjectId() == null) {
        if (transMeta.isUsingClusterSchema(clusterSchema)) {
          repo.save(clusterSchema, versionComment, null);
        }
      }
    }

    // Save the partition schemas
    //
    for (PartitionSchema partitionSchema : transMeta.getPartitionSchemas()) {
      if (partitionSchema.hasChanged() || partitionSchema.getObjectId() == null) {
        if (transMeta.isUsingPartitionSchema(partitionSchema)) {
          repo.save(partitionSchema, versionComment, null);
        }
      }
    }

  }

}
