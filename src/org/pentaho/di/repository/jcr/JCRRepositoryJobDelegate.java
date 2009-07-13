package org.pentaho.di.repository.jcr;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.version.Version;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.jcr.util.JCRObjectVersion;
import org.pentaho.di.shared.SharedObjects;

public class JCRRepositoryJobDelegate extends JCRRepositoryBaseDelegate {
	private static Class<?> PKG = JCRRepository.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private static final String	JOB_HOP_FROM	      = "JOB_HOP_FROM";
	private static final String	JOB_HOP_TO            = "JOB_HOP_TO";
	private static final String	JOB_HOP_ENABLED       = "JOB_HOP_ENABLED";
	private static final String	JOB_HOP_EVALUATION	  = "JOB_HOP_EVALUATION";
	private static final String	JOB_HOP_UNCONDITIONAL = "JOB_HOP_UNCONDITIONAL";
	private static final String	JOB_HOP_PREFIX        = "__JOB_HOP__#";
	
	private static final String	NOTE_PREFIX      = "__NOTE__#";
	
	private static final String	PARAM_PREFIX     = "__PARAM_";
	private static final String	PARAM_KEY        = "KEY_#";
	private static final String	PARAM_DESC       = "DESC_#";
	private static final String	PARAM_DEFAULT    = "DEFAULT_#";


	public JCRRepositoryJobDelegate(JCRRepository repository) {
		super(repository);
	}

	public JobMeta loadJob(String transname, RepositoryDirectory repdir, ProgressMonitorListener monitor, boolean setInternalVariables, String versionLabel) throws KettleException {
		String path = repository.calcRelativeNodePath(repdir, transname, JCRRepository.EXT_JOB); 
		
		try {
			Node node = repository.getRootNode().getNode(path);
			Version version = repository.getVersion(node, versionLabel);
			Node jobNode = repository.getVersionNode(version);
			
			JobMeta jobMeta = new JobMeta();
			
			jobMeta.setName( repository.getObjectName(jobNode));
			jobMeta.setDescription( repository.getObjectDescription(jobNode));
			
			// Grab the Version comment...
			//
			jobMeta.setObjectVersion( repository.getObjectVersion(version) );

			// Get the unique ID
			//
			ObjectId objectId = new StringObjectId(node.getUUID());
			jobMeta.setObjectId(objectId);
						
			// Read the shared objects from the repository, set the shared objects file, etc.
			//
			jobMeta.setSharedObjects(readTransSharedObjects(jobMeta));
			jobMeta.setRepositoryLock( repository.getJobLock(objectId) );
			
			// read the steps...
			//
			NodeIterator nodes = jobNode.getNodes();
			while (nodes.hasNext()) {
				Node jobEntryNode = nodes.nextNode();
				String name = jobEntryNode.getName();
				if (name.endsWith(JCRRepository.EXT_JOB_ENTRY) && !name.startsWith(JOB_HOP_PREFIX)) {
					// This is a job entry node...
					//
					// TODO read the job entry details from the node...
					//
					/*
					JobEntryCopy jobEntryCopy = new JobEntryCopy();
					jobEntryCopy.setObjectId(new StringObjectId(jobEntryNode.getUUID()));
					
					// Read the basics
					//
					jobEntryCopy.setName( repository.getObjectName(jobEntryNode) );
					stepMeta.setDescription( repository.getObjectDescription(jobEntryNode) );
					stepMeta.setDistributes( repository.getPropertyBoolean(jobEntryNode, "STEP_DISTRIBUTE")  );
					stepMeta.setDraw( repository.getPropertyBoolean(jobEntryNode, "STEP_GUI_DRAW")  );
					stepMeta.setCopies( (int) repository.getPropertyLong(jobEntryNode, "STEP_COPIES") );
					
					int x = (int)repository.getPropertyLong(jobEntryNode, "STEP_GUI_LOCATION_X");
					int y = (int)repository.getPropertyLong(jobEntryNode, "STEP_GUI_LOCATION_Y");
					stepMeta.setLocation(x, y);
					
					String stepType = repository.getPropertyString(jobEntryNode, "STEP_TYPE");
					
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
					stepMetaInterface.readRep(repository, stepMeta.getObjectId(), jobMeta.getDatabases(), jobMeta.getCounters());
					stepMeta.setStepMetaInterface(stepMetaInterface);
					
	                // Get the partitioning as well...
					stepMeta.setStepPartitioningMeta( loadStepPartitioningMeta(stepMeta.getObjectId()) );
	                
	                // Get the cluster schema name
	                stepMeta.setClusterSchemaName( repository.getStepAttributeString(stepMeta.getObjectId(), "cluster_schema") );
	                
	                jobMeta.addStep(stepMeta);
	                */
				}
			}

			// Read the notes...
			//
			int nrNotes = (int)jobNode.getProperty("NR_NOTES").getLong();
			nodes = jobNode.getNodes();
			while (nodes.hasNext()) {
				Node noteNode = nodes.nextNode();
				String name = noteNode.getName();
				if (!name.endsWith(JCRRepository.EXT_JOB_ENTRY) && name.startsWith(NOTE_PREFIX)) {
					String xml = repository.getPropertyString( noteNode, JCRRepository.PROPERTY_XML);
					jobMeta.addNote( new NotePadMeta(XMLHandler.getSubNode(XMLHandler.loadXMLString(xml), NotePadMeta.XML_TAG)) );
				}

			}
			if (jobMeta.nrNotes() != nrNotes) {
				throw new KettleException("The number of notes read ["+jobMeta.nrNotes()+"] was not the number we expected ["+nrNotes+"]");
			}

			// Read the hops...
			//
			int nrHops = (int)jobNode.getProperty("NR_HOPS").getLong();
			nodes = jobNode.getNodes();
			while (nodes.hasNext()) {
				Node hopNode = nodes.nextNode();
				String name = hopNode.getName();
				if (!name.endsWith(JCRRepository.EXT_JOB_ENTRY) && name.startsWith(JOB_HOP_PREFIX)) {
					String copyFromId = repository.getPropertyString( hopNode, JOB_HOP_FROM);
					String copyToId = repository.getPropertyString( hopNode, JOB_HOP_TO);
					boolean enabled = repository.getPropertyBoolean( hopNode, JOB_HOP_ENABLED, true);
					
					JobEntryCopy copyFrom = JobMeta.findJobEntryCopy(jobMeta.getJobCopies(), new StringObjectId(copyFromId));
					JobEntryCopy copyTo = JobMeta.findJobEntryCopy(jobMeta.getJobCopies(), new StringObjectId(copyToId));
					
					JobHopMeta jobHopMeta = new JobHopMeta(copyFrom, copyTo);
					jobHopMeta.setEnabled(enabled);
					
					jobMeta.addJobHop( jobHopMeta );
				}

			}
			if (jobMeta.nrJobHops() != nrHops) {
				throw new KettleException("The number of hops read ["+jobMeta.nrJobHops()+"] was not the number we expected ["+nrHops+"]");
			}

			// Load the details at the end, to make sure we reference the databases correctly, etc.
			//
			loadJobDetails(jobNode, jobMeta);

			loadRepParameters(jobNode, jobMeta);
			
			return jobMeta;
		}
		catch(Exception e) {
			throw new KettleException("Unable to load transformation from JCR workspace path ["+path+"]", e);
		}	
	}
	
	private void loadJobDetails(Node jobNode, JobMeta jobMeta) throws KettleException {
		try {
			
		} catch(Exception e) {
			throw new KettleException("Error loading job details", e);
		}
		
	}

	private void loadRepParameters(Node transNode, JobMeta jobMeta) throws KettleException {
		try {
			jobMeta.eraseParameters();
			
			int count = (int)transNode.getProperty("NR_PARAMETERS").getLong();
			for (int idx = 0; idx < count; idx++) {
				String key = repository.getPropertyString(transNode, PARAM_PREFIX+PARAM_KEY+idx);
				String def = repository.getPropertyString(transNode, PARAM_PREFIX+PARAM_DEFAULT+idx);
				String desc = repository.getPropertyString(transNode, PARAM_PREFIX+PARAM_DESC+idx);
				jobMeta.addParameterDefinition(key, def, desc);
			}
    	} catch(Exception e) {
    		throw new KettleException("Unable to load job parameters", e);
    	}

	}   

	public SharedObjects readTransSharedObjects(JobMeta jobMeta) throws KettleException {
		
    	jobMeta.setSharedObjects( jobMeta.readSharedObjects() );
    	
    	// Repository objects take priority so let's overwrite them...
    	//
        readDatabases(jobMeta, true);
        readSlaves(jobMeta, true);
        
        return jobMeta.getSharedObjects();
    }

	   /**
	    * Read all the databases from the repository, insert into the JobMeta object, overwriting optionally
	    * 
	    * @param JobMeta The transformation to load into.
	    * @param overWriteShared if an object with the same name exists, overwrite
	    * @throws KettleException 
	    */
	   public void readDatabases(JobMeta jobMeta, boolean overWriteShared) throws KettleException
	   {
	       try
	       {
	           ObjectId dbids[] = repository.getDatabaseIDs(false);
	           for (int i = 0; i < dbids.length; i++)
	           {
	               DatabaseMeta databaseMeta = repository.loadDatabaseMeta(dbids[i], null); // Load the last version
	               databaseMeta.shareVariablesWith(jobMeta);
	               
	               DatabaseMeta check = jobMeta.findDatabase(databaseMeta.getName()); // Check if there already is one in the transformation
	               if (check==null || overWriteShared) // We only add, never overwrite database connections. 
	               {
	                   if (databaseMeta.getName() != null)
	                   {
	                	   jobMeta.addOrReplaceDatabase(databaseMeta);
	                       if (!overWriteShared) databaseMeta.setChanged(false);
	                   }
	               }
	           }
	           jobMeta.clearChanged();
	       }
	       catch (Exception e)
	       {
	           throw new KettleException(BaseMessages.getString(PKG, "JCRRepository.Exception.UnableToReadDatabasesFromRepository"), e); //$NON-NLS-1$
	       }
	   }

	   /**
	    * Read the slave servers in the repository and add them to this transformation if they are not yet present.
	    * @param JobMeta The transformation to load into.
	    * @param overWriteShared if an object with the same name exists, overwrite
	    * @throws KettleException 
	    */
	   public void readSlaves(JobMeta jobMeta, boolean overWriteShared) throws KettleException
	   {
	       try
	       {
	    	   ObjectId dbids[] = repository.getSlaveIDs(false);
	           for (int i = 0; i < dbids.length; i++)
	           {
	               SlaveServer slaveServer = repository.loadSlaveServer(dbids[i], null); // load the last version
	               slaveServer.shareVariablesWith(jobMeta);
	               SlaveServer check = jobMeta.findSlaveServer(slaveServer.getName()); // Check if there already is one in the transformation
	               if (check==null || overWriteShared) 
	               {
	                   if (!Const.isEmpty(slaveServer.getName()))
	                   {
	                	   jobMeta.addOrReplaceSlaveServer(slaveServer);
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

	
	public void deleteJob(ObjectId jobId) throws KettleException {
		try {
			repository.deleteObject(jobId);
		} catch(Exception e) {
			throw new KettleException("Unable to delete job with ID ["+jobId+"]", e);
		}
	}

	public void saveJob(RepositoryElementInterface element, String versionComment, ProgressMonitorListener monitor) throws KettleException {
		try {
			JobMeta jobMeta = (JobMeta)element;

			// Create or version a new job node
			//
			Node jobNode = repository.createOrVersionNode(element, versionComment);
			
			// Now store the databases in the job.
			// Only store if the database has actually changed or doesn't have an object ID (imported)
			//
			for (DatabaseMeta databaseMeta : jobMeta.getDatabases()) {
				if (databaseMeta.hasChanged() || databaseMeta.getObjectId()==null) {
					
					// Only save the connection if it's actually used in the transformation...
					//
					if (jobMeta.isDatabaseConnectionUsed(databaseMeta)) {
						repository.save(databaseMeta, versionComment, monitor);
					}
				}
			}
			
			for (SlaveServer slaveServer : jobMeta.getSlaveServers()) {
				if (slaveServer.hasChanged() || slaveServer.getObjectId()==null) {
					repository.saveAsXML(slaveServer.getXML(), slaveServer, versionComment, monitor, true);
				}
			}
			
			// Also save all the job entries in the transformation!
			//
			/*
			for (StepMeta step : jobMeta.getSteps()) {
				
				Node stepNode = jobNode.addNode(step.getName()+JCRRepository.EXT_STEP, JCRRepository.NODE_TYPE_UNSTRUCTURED);
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
				saveStepPartitioningMeta(partitioningMeta, jobMeta.getObjectId(), step.getObjectId());
				
				// Save the clustering information as well...
				//
	            repository.saveStepAttribute(jobMeta.getObjectId(), step.getObjectId(), "cluster_schema", step.getClusterSchema()==null?"":step.getClusterSchema().getName());
			}
			
			for (StepMeta step : jobMeta.getSteps()) {
				StepErrorMeta stepErrorMeta = step.getStepErrorMeta();
				if (stepErrorMeta!=null) {
					saveStepErrorMeta(stepErrorMeta, jobMeta.getObjectId(), step.getObjectId());
				}
			}
			*/
			
			// Save the notes
			//
			jobNode.setProperty("NR_NOTES", jobMeta.nrNotes());
			for (int i=0;i<jobMeta.nrNotes();i++) {
				NotePadMeta note = jobMeta.getNote(i);
				Node noteNode = jobNode.addNode(NOTE_PREFIX+i, JCRRepository.NODE_TYPE_UNSTRUCTURED);
				
				noteNode.setProperty(JCRRepository.PROPERTY_XML, note.getXML());
			}

			// Finally, save the hops
			//
			jobNode.setProperty("NR_HOPS", jobMeta.nrJobHops());
			for (int i=0;i<jobMeta.nrJobHops();i++) {
				JobHopMeta hop = jobMeta.getJobHop(i);
				Node hopNode = jobNode.addNode(JOB_HOP_PREFIX+i, JCRRepository.NODE_TYPE_UNSTRUCTURED);
				
				hopNode.setProperty(JOB_HOP_FROM, hop.getFromEntry().getObjectId().getId());
				hopNode.setProperty(JOB_HOP_TO, hop.getToEntry().getObjectId().getId());
				hopNode.setProperty(JOB_HOP_ENABLED, hop.isEnabled());
				hopNode.setProperty(JOB_HOP_EVALUATION, hop.getEvaluation());
				hopNode.setProperty(JOB_HOP_UNCONDITIONAL, hop.isUnconditional());
			}

			saveJobParameters(jobNode, jobMeta);
			
			// Let's not forget to save the details of the transformation itself.
			// This includes logging information, parameters, etc.
			//
			saveTransformationDetails(jobNode, jobMeta, versionComment, monitor);

			// Save the changes to all the steps
			//
            repository.getSession().save();
            jobMeta.setObjectId( new StringObjectId(jobNode.getUUID()));
			Version version = jobNode.checkin();
			jobMeta.setObjectVersion(new JCRObjectVersion(version, versionComment, repository.getUserInfo().getLogin()));
		} catch(Exception e) {
			throw new KettleException("Unable to save job in the JCR repository", e);
		}
	}	
	
    private void saveTransformationDetails(Node jobNode, JobMeta jobMeta, String versionComment, ProgressMonitorListener monitor) {
		// TODO Auto-generated method stub
		
	}

	private void saveJobParameters(Node transNode, JobMeta jobMeta) throws KettleException
    {
    	try {
	    	String[] paramKeys = jobMeta.listParameters();
	    	transNode.setProperty("NR_PARAMETERS", paramKeys==null ? 0 : paramKeys.length);
	    	
	    	for (int idx = 0; idx < paramKeys.length; idx++)  {
	    		String key = paramKeys[idx];
	    		String description = jobMeta.getParameterDescription(paramKeys[idx]);
	    		String defaultValue = jobMeta.getParameterDefault(paramKeys[idx]);
	
	    	    transNode.setProperty(PARAM_PREFIX+PARAM_KEY+idx, key != null ? key : "");		
	    	    transNode.setProperty(PARAM_PREFIX+PARAM_DEFAULT+idx, defaultValue != null ? defaultValue : "");
	    	    transNode.setProperty(PARAM_PREFIX+PARAM_DESC+idx, description != null ? description : "");
	    	}
    	} catch(Exception e) {
    		throw new KettleException("Unable to store job parameters", e);
    	}
    }

}
