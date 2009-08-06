package org.pentaho.di.repository.jcr;

import java.util.Calendar;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Session;
import javax.jcr.version.Version;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepLoaderException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.jcr.util.JCRObjectRevision;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.StepLoader;
import org.pentaho.di.trans.StepPlugin;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepErrorMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepPartitioningMeta;

public class JCRRepositoryTransDelegate extends JCRRepositoryBaseDelegate {
	
	private static Class<?> PKG = JCRRepository.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private static final String	TRANS_HOP_FROM	   = "TRANS_HOP_FROM";
	private static final String	TRANS_HOP_TO       = "TRANS_HOP_TO";
	private static final String	TRANS_HOP_ENABLED  = "TRANS_HOP_ENABLED";
	private static final String	TRANS_HOP_PREFIX   = "__TRANS_HOP__#";
	private static final String	NOTE_PREFIX        = "__NOTE__#";
	private static final String	PARAM_PREFIX       = "__PARAM_";
	private static final String	PARAM_KEY          = "KEY_#";
	private static final String	PARAM_DESC         = "DESC_#";
	private static final String	PARAM_DEFAULT      = "DEFAULT_#";

	public JCRRepositoryTransDelegate(JCRRepository repository) {
		super(repository);
	}

	public void saveTransMeta(RepositoryElementInterface element, String versionComment, ProgressMonitorListener monitor) throws KettleException {
		try {
			TransMeta transMeta = (TransMeta)element;

			
			// First store the databases and other depending objects in the transformation.
			//
			
			// Only store if the database has actually changed or doesn't have an object ID (imported)
			//
			for (DatabaseMeta databaseMeta : transMeta.getDatabases()) {
				if (databaseMeta.hasChanged() || databaseMeta.getObjectId()==null) {
					
					// Only save the connection if it's actually used in the transformation...
					//
					if (transMeta.isDatabaseConnectionUsed(databaseMeta)) {
						repository.save(databaseMeta, versionComment, monitor);
					}
				}
			}
			
			// Store the slave servers...
			//
			for (SlaveServer slaveServer : transMeta.getSlaveServers()) {
				if (slaveServer.hasChanged() || slaveServer.getObjectId()==null) {
					if (transMeta.isUsingSlaveServer(slaveServer)) {
						repository.save(slaveServer, versionComment, monitor);
					}
				}
			}

			// Store the cluster schemas
			//
			for (ClusterSchema clusterSchema : transMeta.getClusterSchemas()) {
				if (clusterSchema.hasChanged() || clusterSchema.getObjectId()==null) {
					if (transMeta.isUsingClusterSchema(clusterSchema)) {
						repository.save(clusterSchema, versionComment, monitor);
					}
				}
			}

			// Save the partition schemas
			//
			for (PartitionSchema partitionSchema : transMeta.getPartitionSchemas()) {
				if (partitionSchema.hasChanged() || partitionSchema.getObjectId()==null) {
					if (transMeta.isUsingPartitionSchema(partitionSchema)) {
						repository.save(partitionSchema, versionComment, monitor);
					}
				}
			}
			
			// Now create or version a new transformation node to place all the data in
			//
			Node transNode = repository.createOrVersionNode(element, versionComment);

			// Also save all the steps in the transformation!
			//
			for (StepMeta step : transMeta.getSteps()) {
				
				Node stepNode = transNode.addNode(step.getName()+JCRRepository.EXT_STEP, JCRRepository.NODE_TYPE_UNSTRUCTURED);
				stepNode.addMixin(JCRRepository.MIX_REFERENCEABLE);
				
				stepNode.setProperty(JCRRepository.PROPERTY_DESCRIPTION, step.getDescription());

				ObjectId id = new StringObjectId(stepNode.getUUID());
				step.setObjectId( id );
				
				// Store the main data
				//
				stepNode.setProperty(JCRRepository.PROPERTY_NAME, step.getName());
				stepNode.setProperty(JCRRepository.PROPERTY_DESCRIPTION, step.getDescription());
				stepNode.setProperty("STEP_TYPE", step.getStepID());
				stepNode.setProperty("STEP_DISTRIBUTE", step.isDistributes());
				stepNode.setProperty("STEP_COPIES", step.getCopies());
				stepNode.setProperty("STEP_GUI_LOCATION_X", step.getLocation().x);
				stepNode.setProperty("STEP_GUI_LOCATION_Y", step.getLocation().y);
				stepNode.setProperty("STEP_GUI_DRAW", step.isDrawn());
				
				// Save the step metadata using the repository save method, NOT XML
				// That is because we want to keep the links to databases, conditions, etc by ID, not name.
				//
				StepMetaInterface stepMetaInterface = step.getStepMetaInterface();
				stepMetaInterface.saveRep(repository, element.getObjectId(), step.getObjectId());
				
				// Save the partitioning information by reference as well...
				//
				StepPartitioningMeta partitioningMeta = step.getStepPartitioningMeta();
				saveStepPartitioningMeta(partitioningMeta, transMeta.getObjectId(), step.getObjectId());
				
				// Save the clustering information as well...
				//
	            repository.saveStepAttribute(transMeta.getObjectId(), step.getObjectId(), "cluster_schema", step.getClusterSchema()==null?"":step.getClusterSchema().getName());
			}
			
			// Save the error hop metadata
			//
			for (StepMeta step : transMeta.getSteps()) {
				StepErrorMeta stepErrorMeta = step.getStepErrorMeta();
				if (stepErrorMeta!=null) {
					saveStepErrorMeta(stepErrorMeta, transMeta.getObjectId(), step.getObjectId());
				}
			}

			// Save the notes
			//
			transNode.setProperty("NR_NOTES", transMeta.nrNotes());
			for (int i=0;i<transMeta.nrNotes();i++) {
				NotePadMeta note = transMeta.getNote(i);
				Node noteNode = transNode.addNode(NOTE_PREFIX+i, JCRRepository.NODE_TYPE_UNSTRUCTURED);
				
				noteNode.setProperty(JCRRepository.PROPERTY_XML, note.getXML());
			}

			// Finally, save the hops
			//
			transNode.setProperty("NR_HOPS", transMeta.nrTransHops());
			for (int i=0;i<transMeta.nrTransHops();i++) {
				TransHopMeta hop = transMeta.getTransHop(i);
				Node hopNode = transNode.addNode(TRANS_HOP_PREFIX+i, JCRRepository.NODE_TYPE_UNSTRUCTURED);
				
				System.out.println("Hop #"+i+" : "+hop.getFromStep().getObjectId()+" --> "+hop.getToStep().getObjectId());
				
				hopNode.setProperty(TRANS_HOP_FROM, hop.getFromStep().getName());
				hopNode.setProperty(TRANS_HOP_TO, hop.getToStep().getName());
				hopNode.setProperty(TRANS_HOP_ENABLED, hop.isEnabled());
			}

			saveTransParameters(transNode, transMeta);
			
			// Let's not forget to save the details of the transformation itself.
			// This includes logging information, parameters, etc.
			//
			saveTransformationDetails(transNode, transMeta, versionComment, monitor);

			// Save the changes to all the steps
			//
            repository.getSession().save();
            transMeta.setObjectId( new StringObjectId(transNode.getUUID()));
			Version version = transNode.checkin();
			transMeta.setObjectRevision(new JCRObjectRevision(version, versionComment, repository.getUserInfo().getLogin()));
			
			transMeta.clearChanged();
		} catch(Exception e) {
			throw new KettleException("Unable to save transformation in the JCR repository", e);
		}
	}

	
	

	private Node saveTransformationDetails(Node node, TransMeta transMeta, String versionComment, ProgressMonitorListener monitor) throws KettleException {
		try {
			Session session = repository.getSession();
			
			node.setProperty("EXTENDED_DESCRIPTION", transMeta.getExtendedDescription());
			node.setProperty("TRANS_VERSION", transMeta.getTransversion());
			node.setProperty("TRANS_STATUS", transMeta.getTransstatus()  <0 ? -1L : transMeta.getTransstatus());
			
			node.setProperty("STEP_READ", transMeta.getReadStep()  ==null ? null : transMeta.getReadStep().getName());
			node.setProperty("STEP_WRITE", transMeta.getWriteStep() ==null ? null : transMeta.getWriteStep().getName());
			node.setProperty("STEP_INPUT", transMeta.getInputStep() ==null ? null : transMeta.getInputStep().getName());
			node.setProperty("STEP_OUTPUT", transMeta.getOutputStep()==null ? null : transMeta.getOutputStep().getName());
			node.setProperty("STEP_UPDATE", transMeta.getUpdateStep()==null ? null : transMeta.getUpdateStep().getName());
			node.setProperty("STEP_REJECTED", transMeta.getRejectedStep()==null ?  null : transMeta.getRejectedStep().getName());

			node.setProperty("DATABASE_LOG", transMeta.getLogConnection()==null ? null : session.getNodeByUUID( transMeta.getLogConnection().getObjectId().getId()) );
			node.setProperty("TABLE_NAME_LOG", transMeta.getLogTable());
			node.setProperty("USE_BATCHID", Boolean.valueOf(transMeta.isBatchIdUsed()));
			node.setProperty("USE_LOGFIELD", Boolean.valueOf(transMeta.isLogfieldUsed()));
			node.setProperty("ID_DATABASE_MAXDATE", transMeta.getMaxDateConnection()==null ? null : session.getNodeByUUID( transMeta.getMaxDateConnection().getObjectId().getId()) );
			node.setProperty("TABLE_NAME_MAXDATE", transMeta.getMaxDateTable());
			node.setProperty("FIELD_NAME_MAXDATE", transMeta.getMaxDateField());
			node.setProperty("OFFSET_MAXDATE", new Double(transMeta.getMaxDateOffset()));
			node.setProperty("DIFF_MAXDATE", new Double(transMeta.getMaxDateDifference()));

			node.setProperty("CREATED_USER", transMeta.getCreatedUser());
			Calendar createdDate = Calendar.getInstance();
			createdDate.setTime(transMeta.getCreatedDate());
			node.setProperty("CREATED_DATE", createdDate);
			
			node.setProperty("MODIFIED_USER", transMeta.getModifiedUser());
			Calendar modifiedDate = Calendar.getInstance();
			createdDate.setTime(transMeta.getModifiedDate());
			node.setProperty("MODIFIED_DATE", modifiedDate);

			node.setProperty("SIZE_ROWSET", transMeta.getSizeRowset());

			node.setProperty("UNIQUE_CONNECTIONS", transMeta.isUsingUniqueConnections());
			node.setProperty("FEEDBACK_SHOWN", transMeta.isFeedbackShown());
	        node.setProperty("FEEDBACK_SIZE", transMeta.getFeedbackSize());
	        node.setProperty("USING_THREAD_PRIORITIES", transMeta.isUsingThreadPriorityManagment());
	        node.setProperty("SHARED_FILE", transMeta.getSharedObjectsFile());
	        
	        node.setProperty("CAPTURE_STEP_PERFORMANCE", transMeta.isCapturingStepPerformanceSnapShots());
	        node.setProperty("STEP_PERFORMANCE_CAPTURING_DELAY", transMeta.getStepPerformanceCapturingDelay());
	        node.setProperty("STEP_PERFORMANCE_LOG_TABLE", transMeta.getStepPerformanceLogTable());

	        node.setProperty("LOG_SIZE_LIMIT", transMeta.getLogSizeLimit());

			return node;
		} catch(Exception e) {
			throw new KettleException("Error saving transformation details", e);
		}
	}
	
	public TransMeta loadTransformation(String transname, RepositoryDirectory repdir, ProgressMonitorListener monitor, boolean setInternalVariables, String versionLabel) throws KettleException {

		String path = repository.calcRelativeNodePath(repdir, transname, JCRRepository.EXT_TRANSFORMATION); 
		
		try {
			Node node = repository.getRootNode().getNode(path);
			Version version = repository.getVersion(node, versionLabel);
			Node transNode = repository.getVersionNode(version);
			
			boolean deleted = repository.getPropertyBoolean(transNode, JCRRepository.PROPERTY_DELETED);
			if (Const.isEmpty(versionLabel) && deleted) {
				// The last version is not available : can't be found!
				//
				throw new KettleException("Transformation ["+transname+"] in directory ["+repdir.getPath()+" can't be found because it's deleted!");
			}
			
			TransMeta transMeta = new TransMeta();
			
			transMeta.setName( repository.getObjectName(transNode));
			transMeta.setDescription( repository.getObjectDescription(transNode));
			transMeta.setRepositoryDirectory(repdir);
			
			// Grab the Version comment...
			//
			transMeta.setObjectRevision( repository.getObjectRevision(version) );

			// Get the unique ID
			//
			ObjectId objectId = new StringObjectId(node.getUUID());
			transMeta.setObjectId(objectId);
						
			// Read the shared objects from the repository, set the shared objects file, etc.
			//
			transMeta.setSharedObjects(readTransSharedObjects(transMeta));
			transMeta.setRepositoryLock( repository.getTransformationLock(objectId) );
			
			// read the steps...
			//
			NodeIterator nodes = transNode.getNodes();
			while (nodes.hasNext()) {
				Node stepNode = nodes.nextNode();
				String name = stepNode.getName();
				if (name.endsWith(JCRRepository.EXT_STEP) && !name.startsWith(TRANS_HOP_PREFIX)) {
					// This is a step node...
					//
					StepMeta stepMeta = new StepMeta(new StringObjectId(stepNode.getUUID()));

					// Read the basics
					//
					stepMeta.setName( repository.getObjectName(stepNode) );
					stepMeta.setDescription( repository.getObjectDescription(stepNode) );
					stepMeta.setDistributes( repository.getPropertyBoolean(stepNode, "STEP_DISTRIBUTE")  );
					stepMeta.setDraw( repository.getPropertyBoolean(stepNode, "STEP_GUI_DRAW")  );
					stepMeta.setCopies( (int) repository.getPropertyLong(stepNode, "STEP_COPIES") );
					
					int x = (int)repository.getPropertyLong(stepNode, "STEP_GUI_LOCATION_X");
					int y = (int)repository.getPropertyLong(stepNode, "STEP_GUI_LOCATION_Y");
					stepMeta.setLocation(x, y);
					
					String stepType = repository.getPropertyString(stepNode, "STEP_TYPE");
					
					// Create a new StepMetaInterface object...
					//
					StepPlugin sp = StepLoader.getInstance().findStepPluginWithID(stepType);
					StepMetaInterface stepMetaInterface = null;
		            if (sp!=null)
		            {
		                stepMetaInterface = BaseStep.getStepInfo(sp, StepLoader.getInstance());
		                stepType=sp.getID()[0]; // revert to the default in case we loaded an alternate version
		            }
		            else
		            {
		                throw new KettleStepLoaderException(BaseMessages.getString(PKG, "StepMeta.Exception.UnableToLoadClass", stepType)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		            }
					
					stepMeta.setStepID(stepType);
					
					// Read the metadata from the repository too...
					//
					stepMetaInterface.readRep(repository, stepMeta.getObjectId(), transMeta.getDatabases(), transMeta.getCounters());
					stepMeta.setStepMetaInterface(stepMetaInterface);
					
	                // Get the partitioning as well...
					stepMeta.setStepPartitioningMeta( loadStepPartitioningMeta(stepMeta.getObjectId()) );
	                
					stepMeta.getStepPartitioningMeta().setPartitionSchemaAfterLoading(transMeta.getPartitionSchemas());
	                // Get the cluster schema name
	                stepMeta.setClusterSchemaName( repository.getStepAttributeString(stepMeta.getObjectId(), "cluster_schema") );
	                
	                transMeta.addStep(stepMeta);
				}
			}

			// Read the notes...
			//
			int nrNotes = (int)transNode.getProperty("NR_NOTES").getLong();
			nodes = transNode.getNodes();
			while (nodes.hasNext()) {
				Node noteNode = nodes.nextNode();
				String name = noteNode.getName();
				if (!name.endsWith(JCRRepository.EXT_STEP) && name.startsWith(NOTE_PREFIX)) {
					String xml = repository.getPropertyString( noteNode, JCRRepository.PROPERTY_XML);
					transMeta.addNote( new NotePadMeta(XMLHandler.getSubNode(XMLHandler.loadXMLString(xml), NotePadMeta.XML_TAG)) );
				}

			}
			if (transMeta.nrNotes() != nrNotes) {
				throw new KettleException("The number of notes read ["+transMeta.nrNotes()+"] was not the number we expected ["+nrNotes+"]");
			}

			// Read the hops...
			//
			int nrHops = (int)transNode.getProperty("NR_HOPS").getLong();
			nodes = transNode.getNodes();
			while (nodes.hasNext()) {
				Node hopNode = nodes.nextNode();
				String name = hopNode.getName();
				if (!name.endsWith(JCRRepository.EXT_STEP) && name.startsWith(TRANS_HOP_PREFIX)) {
					String stepFromName = repository.getPropertyString( hopNode, TRANS_HOP_FROM);
					String stepToName = repository.getPropertyString( hopNode, TRANS_HOP_TO);
					boolean enabled = repository.getPropertyBoolean( hopNode, TRANS_HOP_ENABLED, true);
					
					System.out.println("Read Hop from "+stepFromName+" --> "+stepToName);

					StepMeta stepFrom = StepMeta.findStep(transMeta.getSteps(), stepFromName);
					StepMeta stepTo = StepMeta.findStep(transMeta.getSteps(), stepToName);
					
					transMeta.addTransHop( new TransHopMeta(stepFrom, stepTo, enabled) );
				}

			}
			if (transMeta.nrTransHops() != nrHops) {
				throw new KettleException("The number of hops read ["+transMeta.nrTransHops()+"] was not the number we expected ["+nrHops+"]");
			}
			
            // Also load the step error handling metadata
            //
            for (int i=0;i<transMeta.nrSteps();i++)
            {
                StepMeta stepMeta = transMeta.getStep(i);
                String sourceStep = repository.getStepAttributeString(stepMeta.getObjectId(), "step_error_handling_source_step");
                if (sourceStep!=null)
                {
                    StepErrorMeta stepErrorMeta = loadStepErrorMeta(transMeta, stepMeta, transMeta.getSteps());
                    stepErrorMeta.getSourceStep().setStepErrorMeta(stepErrorMeta); // a bit of a trick, I know.                        
                }
            }

			// Load the details at the end, to make sure we reference the databases correctly, etc.
			//
			loadTransformationDetails(transNode, transMeta);

			loadRepParameters(transNode, transMeta);
			
			return transMeta;
		}
		catch(Exception e) {
			throw new KettleException("Unable to load transformation from JCR workspace path ["+path+"]", e);
		}
	}
		
    private StepErrorMeta loadStepErrorMeta(VariableSpace variables, StepMeta stepMeta, List<StepMeta> steps) throws KettleException
    {
    	StepErrorMeta meta = new StepErrorMeta(variables, stepMeta);
    	
    	meta.setTargetStep( StepMeta.findStep( steps, repository.getStepAttributeString(stepMeta.getObjectId(), "step_error_handling_target_step") ) );
    	meta.setEnabled( repository.getStepAttributeBoolean(stepMeta.getObjectId(), "step_error_handling_is_enabled") );
    	meta.setNrErrorsValuename( repository.getStepAttributeString(stepMeta.getObjectId(), "step_error_handling_nr_valuename") );
    	meta.setErrorDescriptionsValuename(repository.getStepAttributeString(stepMeta.getObjectId(), "step_error_handling_descriptions_valuename") );
    	meta.setErrorFieldsValuename( repository.getStepAttributeString(stepMeta.getObjectId(), "step_error_handling_fields_valuename") );
    	meta.setErrorCodesValuename( repository.getStepAttributeString(stepMeta.getObjectId(), "step_error_handling_codes_valuename") );
    	meta.setMaxErrors( repository.getStepAttributeInteger(stepMeta.getObjectId(), "step_error_handling_max_errors") );
    	meta.setMaxPercentErrors( (int) repository.getStepAttributeInteger(stepMeta.getObjectId(), "step_error_handling_max_pct_errors") );
    	meta.setMinPercentRows( repository.getStepAttributeInteger(stepMeta.getObjectId(), "step_error_handling_min_pct_rows") );
    	
    	return meta;
    }

	public void loadTransformationDetails(Node node, TransMeta transMeta) throws KettleException {
		try {
            transMeta.setExtendedDescription( repository.getPropertyString(node, "EXTENDED_DESCRIPTION") );
            transMeta.setTransversion( repository.getPropertyString(node, "TRANS_VERSION") );
			transMeta.setTransstatus( (int)repository.getPropertyLong(node, "TRANS_STATUS") );

			transMeta.setReadStep( StepMeta.findStep(transMeta.getSteps(), repository.getPropertyString(node, "STEP_READ")) );  //$NON-NLS-1$
			transMeta.setWriteStep( StepMeta.findStep(transMeta.getSteps(), repository.getPropertyString(node, "STEP_WRITE")) ); //$NON-NLS-1$
			transMeta.setInputStep( StepMeta.findStep(transMeta.getSteps(), repository.getPropertyString(node, "STEP_INPUT")) ); //$NON-NLS-1$
			transMeta.setOutputStep( StepMeta.findStep(transMeta.getSteps(), repository.getPropertyString(node, "STEP_OUTPUT")) ); //$NON-NLS-1$
			transMeta.setUpdateStep( StepMeta.findStep(transMeta.getSteps(), repository.getPropertyString(node, "STEP_UPDATE")) ); //$NON-NLS-1$
			transMeta.setRejectedStep( StepMeta.findStep(transMeta.getSteps(), repository.getPropertyString(node, "STEP_REJECTED")) ); //$NON-NLS-1$

			Node logDatabaseNode = repository.getPropertyNode(node, "DATABASE_LOG");  //$NON-NLS-1$
            transMeta.setLogConnection( logDatabaseNode==null ? null : DatabaseMeta.findDatabase(transMeta.getDatabases(), new StringObjectId(logDatabaseNode.getUUID())) );
            transMeta.setLogTable( repository.getPropertyString(node, "TABLE_NAME_LOG") ); //$NON-NLS-1$
            transMeta.setBatchIdUsed( repository.getPropertyBoolean(node, "USE_BATCHID") ); //$NON-NLS-1$
            transMeta.setLogfieldUsed( repository.getPropertyBoolean(node, "USE_LOGFIELD") ); //$NON-NLS-1$

            Node maxDatabaseNode = repository.getPropertyNode(node, "DATABASE_MAX"); //$NON-NLS-1$
            transMeta.setMaxDateConnection( maxDatabaseNode==null ? null :  DatabaseMeta.findDatabase(transMeta.getDatabases(), new StringObjectId(maxDatabaseNode.getUUID())) ); 
            transMeta.setMaxDateTable( repository.getPropertyString(node, "TABLE_NAME_MAXDATE") ); //$NON-NLS-1$
            transMeta.setMaxDateField( repository.getPropertyString(node, "FIELD_NAME_MAXDATE") ); //$NON-NLS-1$
            transMeta.setMaxDateOffset( repository.getPropertyNumber(node, "OFFSET_MAXDATE") ); //$NON-NLS-1$
            transMeta.setMaxDateDifference( repository.getPropertyNumber(node, "DIFF_MAXDATE") ); //$NON-NLS-1$

            transMeta.setCreatedUser( repository.getPropertyString(node, "CREATED_USER") ); //$NON-NLS-1$
            transMeta.setCreatedDate( repository.getPropertyDate(node, "CREATED_DATE") ); //$NON-NLS-1$

            transMeta.setModifiedUser( repository.getPropertyString(node, "MODIFIED_USER") ); //$NON-NLS-1$
            transMeta.setModifiedDate( repository.getPropertyDate(node, "MODIFIED_DATE") ); //$NON-NLS-1$

            // Optional:
            transMeta.setSizeRowset( Const.ROWS_IN_ROWSET );
            long val_size_rowset = repository.getPropertyLong(node, "SIZE_ROWSET"); //$NON-NLS-1$
            if (val_size_rowset > 0)
            {
            	transMeta.setSizeRowset( (int)val_size_rowset );
            }

            String id_directory = repository.getPropertyString(node,"ID_DIRECTORY"); //$NON-NLS-1$
            if (id_directory != null)
            {
           	   if (log.isDetailed()) log.logDetailed(toString(), "ID_DIRECTORY=" + id_directory); //$NON-NLS-1$
               // Set right directory...
           	   transMeta.setRepositoryDirectory( repository.loadRepositoryDirectoryTree().findDirectory(new StringObjectId(id_directory)) ); // always reload the folder structure
            }
           
            transMeta.setUsingUniqueConnections( repository.getPropertyBoolean(node, "UNIQUE_CONNECTIONS") );
            transMeta.setFeedbackShown( repository.getPropertyBoolean(node, "FEEDBACK_SHOWN", true)  );
            transMeta.setFeedbackSize( (int) repository.getPropertyLong(node, "FEEDBACK_SIZE") );
            transMeta.setUsingThreadPriorityManagment( repository.getPropertyBoolean(node, "USING_THREAD_PRIORITIES", true) );    
           
            // Performance monitoring for steps...
            //
            transMeta.setCapturingStepPerformanceSnapShots( repository.getPropertyBoolean(node, "CAPTURE_STEP_PERFORMANCE", true) );
            transMeta.setStepPerformanceCapturingDelay( repository.getPropertyLong(node, "STEP_PERFORMANCE_CAPTURING_DELAY") );
            transMeta.setStepPerformanceLogTable( repository.getPropertyString(node, "STEP_PERFORMANCE_LOG_TABLE") );
            transMeta.setLogSizeLimit( repository.getPropertyString(node, "LOG_SIZE_LIMIT") );

		} catch(Exception e) {
			throw new KettleException("Error loading transformation details", e);
		}
	}
	
    /**
     * Save the parameters of this transformation to the repository.
     * 
     * @param transMeta the transformation to reference
     * 
     * @throws KettleException Upon any error.
     * 
     */
    private void saveTransParameters(Node transNode, TransMeta transMeta) throws KettleException
    {
    	try {
	    	String[] paramKeys = transMeta.listParameters();
	    	transNode.setProperty("NR_PARAMETERS", paramKeys==null ? 0 : paramKeys.length);
	    	
	    	for (int idx = 0; idx < paramKeys.length; idx++)  {
	    		String key = paramKeys[idx];
	    		String description = transMeta.getParameterDescription(paramKeys[idx]);
	    		String defaultValue = transMeta.getParameterDefault(paramKeys[idx]);
	
	    	    transNode.setProperty(PARAM_PREFIX+PARAM_KEY+idx, key != null ? key : "");		
	    	    transNode.setProperty(PARAM_PREFIX+PARAM_DEFAULT+idx, defaultValue != null ? defaultValue : "");
	    	    transNode.setProperty(PARAM_PREFIX+PARAM_DESC+idx, description != null ? description : "");
	    	}
    	} catch(Exception e) {
    		throw new KettleException("Unable to store transformation parameters", e);
    	}
    }

	 /**
	 * Load the parameters of this transformation from the repository. The
	 * current ones already loaded will be erased.
	 * 
	 * @param rep
	 *            The repository to load from.
	 * 
	 * @throws KettleException
	 *             Upon any error.
	 */
	private void loadRepParameters(Node transNode, TransMeta transMeta) throws KettleException {
		try {
			transMeta.eraseParameters();
			
			int count = (int)transNode.getProperty("NR_PARAMETERS").getLong();
			for (int idx = 0; idx < count; idx++) {
				String key = repository.getPropertyString(transNode, PARAM_PREFIX+PARAM_KEY+idx);
				String def = repository.getPropertyString(transNode, PARAM_PREFIX+PARAM_DEFAULT+idx);
				String desc = repository.getPropertyString(transNode, PARAM_PREFIX+PARAM_DESC+idx);
				transMeta.addParameterDefinition(key, def, desc);
			}
    	} catch(Exception e) {
    		throw new KettleException("Unable to load transformation parameters", e);
    	}
	}   

	public SharedObjects readTransSharedObjects(TransMeta transMeta) throws KettleException {
		
    	transMeta.setSharedObjects( transMeta.readSharedObjects() );
    	
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
   public void readDatabases(TransMeta transMeta, boolean overWriteShared) throws KettleException
   {
       try
       {
           ObjectId dbids[] = repository.getDatabaseIDs(false);
           for (int i = 0; i < dbids.length; i++)
           {
               DatabaseMeta databaseMeta = repository.loadDatabaseMeta(dbids[i], null); // Load the last version
               databaseMeta.shareVariablesWith(transMeta);
               
               DatabaseMeta check = transMeta.findDatabase(databaseMeta.getName()); // Check if there already is one in the transformation
               if (check==null || overWriteShared) // We only add, never overwrite database connections. 
               {
                   if (databaseMeta.getName() != null)
                   {
                	   transMeta.addOrReplaceDatabase(databaseMeta);
                       if (!overWriteShared) databaseMeta.setChanged(false);
                   }
               }
           }
           transMeta.clearChangedDatabases();
       }
       catch (Exception e)
       {
           throw new KettleException(BaseMessages.getString(PKG, "JCRRepository.Exception.UnableToReadDatabasesFromRepository"), e); //$NON-NLS-1$
       }
   }
	
   /**
    * Read the clusters in the repository and add them to this transformation if they are not yet present.
    * @param TransMeta The transformation to load into.
    * @param overWriteShared if an object with the same name exists, overwrite
    * @throws KettleException 
    */
   public void readClusters(TransMeta transMeta, boolean overWriteShared) throws KettleException
   {
       try
       {
           ObjectId dbids[] = repository.getClusterIDs(false);
           for (int i = 0; i < dbids.length; i++)
           {
               ClusterSchema clusterSchema = repository.loadClusterSchema(dbids[i], transMeta.getSlaveServers(), null); // Read the last version
               clusterSchema.shareVariablesWith(transMeta);
               ClusterSchema check = transMeta.findClusterSchema(clusterSchema.getName()); // Check if there already is one in the transformation
               if (check==null || overWriteShared) 
               {
                   if (!Const.isEmpty(clusterSchema.getName()))
                   {
                       transMeta.addOrReplaceClusterSchema(clusterSchema);
                       if (!overWriteShared) clusterSchema.setChanged(false);
                   }
               }
           }
       }
       catch (KettleDatabaseException dbe)
       {
           throw new KettleException(BaseMessages.getString(PKG, "JCRRepository.Log.UnableToReadClustersFromRepository"), dbe); //$NON-NLS-1$
       }
   }
   
   
   /**
    * Read the partitions in the repository and add them to this transformation if they are not yet present.
    * @param TransMeta The transformation to load into.
    * @param overWriteShared if an object with the same name exists, overwrite
    * @throws KettleException 
    */
   public void readPartitionSchemas(TransMeta transMeta, boolean overWriteShared) throws KettleException
   {
       try
       {
    	   ObjectId dbids[] = repository.getPartitionSchemaIDs(false);
           for (int i = 0; i < dbids.length; i++)
           {
               PartitionSchema partitionSchema = repository.loadPartitionSchema(dbids[i], null); // Load the last version
               PartitionSchema check = transMeta.findPartitionSchema(partitionSchema.getName()); // Check if there already is one in the transformation
               if (check==null || overWriteShared) 
               {
                   if (!Const.isEmpty(partitionSchema.getName()))
                   {
                	   transMeta.addOrReplacePartitionSchema(partitionSchema);
                       if (!overWriteShared) partitionSchema.setChanged(false);
                   }
               }
           }
       }
       catch (KettleException dbe)
       {
           throw new KettleException(BaseMessages.getString(PKG, "JCRRepository.Log.UnableToReadPartitionSchemaFromRepository"), dbe); //$NON-NLS-1$
       }
   }

   /**
    * Read the slave servers in the repository and add them to this transformation if they are not yet present.
    * @param TransMeta The transformation to load into.
    * @param overWriteShared if an object with the same name exists, overwrite
    * @throws KettleException 
    */
   public void readSlaves(TransMeta transMeta, boolean overWriteShared) throws KettleException
   {
       try
       {
    	   ObjectId dbids[] = repository.getSlaveIDs(false);
           for (int i = 0; i < dbids.length; i++)
           {
               SlaveServer slaveServer = repository.loadSlaveServer(dbids[i], null); // load the last version
               slaveServer.shareVariablesWith(transMeta);
               SlaveServer check = transMeta.findSlaveServer(slaveServer.getName()); // Check if there already is one in the transformation
               if (check==null || overWriteShared) 
               {
                   if (!Const.isEmpty(slaveServer.getName()))
                   {
                	   transMeta.addOrReplaceSlaveServer(slaveServer);
                       if (!overWriteShared) slaveServer.setChanged(false);
                   }
               }
           }
       }
       catch (KettleDatabaseException dbe)
       {
           throw new KettleException(BaseMessages.getString(PKG, "JCRRepository.Log.UnableToReadSlaveServersFromRepository"), dbe); //$NON-NLS-1$
       }
   }
	
	
	
	
    /**
     * Saves partitioning properties in the repository for the given step.
     * 
     * @param meta the partitioning metadata to store.
     * @param id_transformation the ID of the transformation
     * @param stepId the ID of the step
     * @throws KettleDatabaseException In case anything goes wrong
     * 
     */
    public void saveStepPartitioningMeta(StepPartitioningMeta meta, ObjectId id_transformation, ObjectId stepId) throws KettleException
    {
    	try {
    		if (meta!=null && meta.getPartitionSchema()!=null && meta.isPartitioned()) {
		    	Node partitionSchemaNode = repository.getSession().getNodeByUUID(meta.getPartitionSchema().getObjectId().getId());
		    	
		        repository.saveStepAttribute(id_transformation, stepId, "PARTITIONING_SCHEMA",    partitionSchemaNode);  // reference to selected schema node
		        repository.saveStepAttribute(id_transformation, stepId, "PARTITIONING_METHOD",    meta.getMethodCode()); // method of partitioning
		        if( meta.getPartitioner() != null ) {
		        	meta.getPartitioner().saveRep( repository, id_transformation, stepId);
		        }
    		}
    	} catch(Exception e) {
    		throw new KettleException("Unable to save step partitioning metadata for step with id ["+stepId+"]", e);
    	}
    }
    
    public StepPartitioningMeta loadStepPartitioningMeta(ObjectId stepId) throws KettleException
    {
    	try {
	    	StepPartitioningMeta stepPartitioningMeta = new StepPartitioningMeta();
	    	
	    	Node node = repository.getStepAttributeNode(stepId, "PARTITIONING_SCHEMA");
	    	String schemaName = node==null ? null : repository.getObjectName(node);
	    	
	    	stepPartitioningMeta.setPartitionSchemaName( schemaName );
	        String methodCode   = repository.getStepAttributeString(stepId, "PARTITIONING_METHOD");
	        stepPartitioningMeta.setMethod( StepPartitioningMeta.getMethod(methodCode) );
	        if( stepPartitioningMeta.getPartitioner() != null ) {
	        	stepPartitioningMeta.getPartitioner().loadRep( repository, stepId);
	        }
	        stepPartitioningMeta.hasChanged(true);
	        
	        return stepPartitioningMeta;
    	} catch(Exception e) {
    		throw new KettleException("Unable to step partitioning meta information for step with id ["+stepId+"]", e);
    	}
    }

    
    /**
     * Delete last version of a transformation.
     * 
     * @param transformationId
     * @throws KettleException
     */
	public void deleteTransformation(ObjectId transformationId) throws KettleException {
		try {
			repository.deleteObject(transformationId, RepositoryObjectType.TRANSFORMATION);
		} catch(Exception e) {
			throw new KettleException("Unable to delete transformation with ID ["+transformationId+"]", e);
		}
	}
	
    public void saveStepErrorMeta(StepErrorMeta meta, ObjectId id_transformation, ObjectId stepId) throws KettleException
    {
        repository.saveStepAttribute(id_transformation, stepId, "step_error_handling_source_step", meta.getSourceStep()!=null ? meta.getSourceStep().getName() : "");
        repository.saveStepAttribute(id_transformation, stepId, "step_error_handling_target_step", meta.getTargetStep()!=null ? meta.getTargetStep().getName() : "");
        repository.saveStepAttribute(id_transformation, stepId, "step_error_handling_is_enabled",  meta.isEnabled());
        repository.saveStepAttribute(id_transformation, stepId, "step_error_handling_nr_valuename",  meta.getNrErrorsValuename());
        repository.saveStepAttribute(id_transformation, stepId, "step_error_handling_descriptions_valuename",  meta.getErrorDescriptionsValuename());
        repository.saveStepAttribute(id_transformation, stepId, "step_error_handling_fields_valuename",  meta.getErrorFieldsValuename());
        repository.saveStepAttribute(id_transformation, stepId, "step_error_handling_codes_valuename",  meta.getErrorCodesValuename());
        repository.saveStepAttribute(id_transformation, stepId, "step_error_handling_max_errors",  meta.getMaxErrors());
        repository.saveStepAttribute(id_transformation, stepId, "step_error_handling_max_pct_errors",  meta.getMaxPercentErrors());
        repository.saveStepAttribute(id_transformation, stepId, "step_error_handling_min_pct_rows",  meta.getMinPercentRows());
    }
    
	public ObjectId renameTransformation(ObjectId transformationId, RepositoryDirectory newDir, String newname) throws KettleException {
		try { 
			Node transNode = repository.getSession().getNodeByUUID(transformationId.getId());
			
			// Change the name in the properties...
			//
			transNode.checkout();
			transNode.setProperty(JCRRepository.PROPERTY_NAME, newname);

			// Now change the name of the node itself with a move
			//
			String oldPath = transNode.getPath();
			String newPath = repository.calcNodePath(newDir, newname, JCRRepository.EXT_TRANSFORMATION);
			
			repository.getSession().move(oldPath, newPath);
			repository.getSession().save();
			transNode.checkin();
			
			return transformationId; // same ID, nothing changed
		} catch(Exception e) {
			throw new KettleException("Unable to rename transformation with id ["+transformationId+"] to ["+newname+"]", e);
		}
	}

}
