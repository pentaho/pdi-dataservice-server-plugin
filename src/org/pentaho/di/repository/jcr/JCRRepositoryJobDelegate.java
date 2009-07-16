package org.pentaho.di.repository.jcr;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

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
import org.pentaho.di.job.JobEntryLoader;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.JobPlugin;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.RepositoryObjectType;
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

	public JobMeta loadJobMeta(String jobName, RepositoryDirectory repdir, ProgressMonitorListener monitor, String versionLabel) throws KettleException {
		String path = repository.calcRelativeNodePath(repdir, jobName, JCRRepository.EXT_JOB); 
		
		try {
			Node node = repository.getRootNode().getNode(path);
			Version version = repository.getVersion(node, versionLabel);
			Node jobNode = repository.getVersionNode(version);
			
			JobMeta jobMeta = new JobMeta();
			
			jobMeta.setName( repository.getObjectName(jobNode));
			jobMeta.setDescription( repository.getObjectDescription(jobNode));
			jobMeta.setRepositoryDirectory(repdir);
			
			// Grab the Version comment...
			//
			jobMeta.setObjectVersion( repository.getObjectVersion(version) );

			// Get the unique ID
			//
			ObjectId objectId = new StringObjectId(node.getUUID());
			jobMeta.setObjectId(objectId);
						
			// Read the shared objects from the repository, set the shared objects file, etc.
			// !! make sure to set the shared objects file prior to loading the shared objects !!
			//
			jobMeta.setSharedObjectsFile( repository.getPropertyString(jobNode, "SHARED_FILE") );
			jobMeta.setSharedObjects(readSharedObjects(jobMeta));
			jobMeta.setRepositoryLock( repository.getJobLock(objectId) );
			
			// Keep a unique list of job entries to facilitate in the loading.
			//
			List<JobEntryInterface> jobentries = new ArrayList<JobEntryInterface>();

			// Read the job entry copies
			//
			int nrCopies = (int) jobNode.getProperty("NR_JOB_ENTRY_COPIES").getLong();
			
			// read the copies...
			//
			NodeIterator nodes = jobNode.getNodes();
			while (nodes.hasNext()) {
				Node copyNode = nodes.nextNode();
				String name = copyNode.getName();
				if (name.endsWith(JCRRepository.EXT_JOB_ENTRY_COPY) && !name.startsWith(JOB_HOP_PREFIX)) {
					// This is a job entry copy node...
					
					// Read the entry...
					//
					JobEntryInterface jobEntry = readJobEntry(copyNode, jobMeta, jobentries);
					
					JobEntryCopy copy = new JobEntryCopy(jobEntry);

					copy.setName( repository.getObjectName(copyNode));
					copy.setDescription( repository.getObjectDescription(copyNode));
					copy.setObjectId(new StringObjectId(copyNode.getUUID()));

					copy.setNr( (int)repository.getPropertyLong(copyNode, "NR") );
					int x = (int)repository.getPropertyLong(copyNode, "GUI_LOCATION_X");
					int y = (int)repository.getPropertyLong(copyNode, "GUI_LOCATION_Y");
					copy.setLocation(x, y);
					copy.setDrawn( repository.getPropertyBoolean(copyNode, "GUI_DRAWN") );
					copy.setLaunchingInParallel( repository.getPropertyBoolean(copyNode, "PARALLEL") );
	                
	                jobMeta.getJobCopies().add(copy);
				}
			}

			if (jobMeta.getJobCopies().size() != nrCopies) {
				throw new KettleException("The number of job entry copies read ["+jobMeta.getJobCopies().size()+"] was not the number we expected ["+nrCopies+"]");
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
			loadJobMetaDetails(jobNode, jobMeta);

			loadRepParameters(jobNode, jobMeta);
			
			return jobMeta;
		}
		catch(Exception e) {
			throw new KettleException("Unable to load transformation from JCR workspace path ["+path+"], "+(versionLabel==null ? "the last version":"version "+versionLabel), e);
		}	
	}
	
	private JobEntryInterface readJobEntry(Node entryNode, JobMeta jobMeta, List<JobEntryInterface> jobentries) throws KettleException {
		try  {
			String name = repository.getObjectName(entryNode);
			for (JobEntryInterface entry : jobentries) {
				if (entry.getName().equalsIgnoreCase(name)) {
					return entry; // already loaded!
				}
			}
			
			// load the entry from the node
			//
			String typeId = entryNode.getProperty("JOBENTRY_TYPE").getString();
			JobPlugin plugin = JobEntryLoader.getInstance().findJobPluginWithID(typeId);
			JobEntryInterface entry = JobEntryLoader.getInstance().getJobEntryClass(plugin);
			entry.setName( name );
			entry.setDescription( repository.getObjectDescription(entryNode));
			entry.setObjectId(new StringObjectId(entryNode.getUUID()));
			entry.loadRep(repository, entry.getObjectId(), jobMeta.getDatabases(), jobMeta.getSlaveServers());
            
			jobentries.add(entry);
            
            return entry;
		}
		catch(Exception e) {
			throw new KettleException("Unable to read job entry interface information from repository", e);
		}
	}

	private void loadJobMetaDetails(Node jobNode, JobMeta jobMeta) throws KettleException {
		try {
			jobMeta.setName( repository.getObjectName(jobNode) );
			jobMeta.setDescription( repository.getObjectDescription(jobNode) );
			jobMeta.setExtendedDescription( repository.getPropertyString(jobNode, "EXTENDED_DESCRIPTION") ); //$NON-NLS-1$
			jobMeta.setJobversion( repository.getPropertyString(jobNode, "JOB_VERSION") ); //$NON-NLS-1$
			jobMeta.setJobstatus( (int) repository.getPropertyLong(jobNode, "JOB_STATUS") ); //$NON-NLS-1$
			jobMeta.setLogTable( repository.getPropertyString(jobNode, "TABLE_NAME_LOG") ); //$NON-NLS-1$

			jobMeta.setCreatedUser( repository.getPropertyString(jobNode, "CREATED_USER") ); //$NON-NLS-1$
			jobMeta.setCreatedDate( repository.getPropertyDate(jobNode, "CREATED_DATE") ); //$NON-NLS-1$

			jobMeta.setModifiedUser( repository.getPropertyString(jobNode, "MODIFIED_USER") ); //$NON-NLS-1$
			jobMeta.setModifiedDate( repository.getPropertyDate(jobNode, "MODIFIED_DATE") ); //$NON-NLS-1$

			Node logDbNode = repository.getPropertyNode(jobNode, "DATABASE_LOG");
			if (logDbNode!=null) {
				jobMeta.setLogConnection( DatabaseMeta.findDatabase(jobMeta.getDatabases(), new StringObjectId(logDbNode.getUUID())) );
			}
			jobMeta.setUseBatchId( repository.getPropertyBoolean(jobNode, "USE_BATCH_ID") ); //$NON-NLS-1$
			jobMeta.setBatchIdPassed( repository.getPropertyBoolean(jobNode, "PASS_BATCH_ID") ); //$NON-NLS-1$
			jobMeta.setLogfieldUsed( repository.getPropertyBoolean(jobNode, "USE_LOGFIELD") ); //$NON-NLS-1$
			
			jobMeta.setLogSizeLimit( repository.getPropertyString(jobNode, "LOG_SIZE_LIMIT") );

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

	public SharedObjects readSharedObjects(JobMeta jobMeta) throws KettleException {
		
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
	    * Read the slave servers in the repository and add them to this job if they are not yet present.
	    * @param JobMeta The job to load into.
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
			repository.deleteObject(jobId, RepositoryObjectType.JOB);
		} catch(Exception e) {
			throw new KettleException("Unable to delete job with ID ["+jobId+"]", e);
		}
	}

	public void saveJobMeta(RepositoryElementInterface element, String versionComment, ProgressMonitorListener monitor) throws KettleException {
		try {
			JobMeta jobMeta = (JobMeta)element;

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

			// Store the slave server
			//
			for (SlaveServer slaveServer : jobMeta.getSlaveServers()) {
				if (slaveServer.hasChanged() || slaveServer.getObjectId()==null) {
					repository.save(slaveServer, versionComment, monitor);
				}
			}

			// Create or version a new job node to store all the information in...
			//
			Node jobNode = repository.createOrVersionNode(element, versionComment);

			// Set the object ID on the job
			//
            jobMeta.setObjectId( new StringObjectId(jobNode.getUUID()));

			// Save the notes
			//
			jobNode.setProperty("NR_NOTES", jobMeta.nrNotes());
			for (int i=0;i<jobMeta.nrNotes();i++) {
				NotePadMeta note = jobMeta.getNote(i);
				Node noteNode = jobNode.addNode(NOTE_PREFIX+i, JCRRepository.NODE_TYPE_UNSTRUCTURED);
				
				noteNode.setProperty(JCRRepository.PROPERTY_XML, note.getXML());
			}
			
			//
			// Save the job entry copies
			//
			if(log.isDetailed()) log.logDetailed(toString(), "Saving " + jobMeta.nrJobEntries() + " Job enty copies to repository..."); //$NON-NLS-1$ //$NON-NLS-2$
			jobNode.setProperty("NR_JOB_ENTRY_COPIES", jobMeta.nrJobEntries());
			for (int i = 0; i < jobMeta.nrJobEntries(); i++) {

				JobEntryCopy copy = jobMeta.getJobEntry(i);
				JobEntryInterface entry = copy.getEntry();
				
				// Create a new node for each entry...
				//
				Node copyNode = jobNode.addNode(copy.getName()+JCRRepository.EXT_JOB_ENTRY_COPY, JCRRepository.NODE_TYPE_UNSTRUCTURED);
				copyNode.addMixin(JCRRepository.MIX_REFERENCEABLE);
				
				copy.setObjectId( new StringObjectId(copyNode.getUUID()) );
				entry.setObjectId( copy.getObjectId() );
				
				copyNode.setProperty(JCRRepository.PROPERTY_NAME, copy.getName());
				copyNode.setProperty(JCRRepository.PROPERTY_DESCRIPTION, copy.getDescription());

				copyNode.setProperty("NR", copy.getNr());
				copyNode.setProperty("GUI_LOCATION_X", copy.getLocation().x);
				copyNode.setProperty("GUI_LOCATION_Y", copy.getLocation().y);
				copyNode.setProperty("GUI_DRAW", copy.isDrawn());
				copyNode.setProperty("PARALLEL", copy.isLaunchingInParallel());
				
				// Save the entry information here as well, for completeness.  TODO: since this slightly stores duplicate information, figure out how to store this separately.
				//
				copyNode.setProperty("JOBENTRY_TYPE", entry.getTypeId());
				entry.saveRep(repository, jobMeta.getObjectId());
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
			saveJobDetails(jobNode, jobMeta, versionComment, monitor);

			// Save the changes to all the steps
			//
            repository.getSession().save();
			Version version = jobNode.checkin();
			jobMeta.setObjectVersion(new JCRObjectVersion(version, versionComment, repository.getUserInfo().getLogin()));
			
			jobMeta.clearChanged();
		} catch(Exception e) {
			throw new KettleException("Unable to save job in the JCR repository", e);
		}
	}	
	
    private void saveJobDetails(Node jobNode, JobMeta jobMeta, String versionComment, ProgressMonitorListener monitor) throws KettleException {
    	try {
	    	jobNode.setProperty(JCRRepository.PROPERTY_NAME, jobMeta.getName());
	    	jobNode.setProperty(JCRRepository.PROPERTY_DESCRIPTION, jobMeta.getDescription());
	    	
	    	jobNode.setProperty("EXTENDED_DESCRIPTION", jobMeta.getExtendedDescription());
	    	jobNode.setProperty("JOB_VERSION", jobMeta.getJobversion());
	    	jobNode.setProperty("JOB_STATUS", jobMeta.getJobstatus()  <0 ? -1L : jobMeta.getJobstatus());
	
	    	Node logDbNode = jobMeta.getLogConnection()==null ? null : repository.getSession().getNodeByUUID(jobMeta.getLogConnection().getObjectId().getId());
	    	jobNode.setProperty("DATABASE_LOG", logDbNode);
	    	jobNode.setProperty("TABLE_NAME_LOG", jobMeta.getLogTable());
	
	    	jobNode.setProperty("CREATED_USER", jobMeta.getCreatedUser());
	    	Calendar createdDate = Calendar.getInstance();
	    	createdDate.setTime(jobMeta.getCreatedDate());
	    	jobNode.setProperty("CREATED_DATE", createdDate);
	    	jobNode.setProperty("MODIFIED_USER", jobMeta.getModifiedUser());
	    	Calendar modifiedDate = Calendar.getInstance();
	    	modifiedDate.setTime(jobMeta.getModifiedDate());
	    	jobNode.setProperty("MODIFIED_DATE", modifiedDate);
	    	jobNode.setProperty("USE_BATCH_ID", jobMeta.isBatchIdUsed());
	    	jobNode.setProperty("PASS_BATCH_ID", jobMeta.isBatchIdPassed());
	    	jobNode.setProperty("USE_LOGFIELD", jobMeta.isLogfieldUsed());
	    	jobNode.setProperty("SHARED_FILE", jobMeta.getSharedObjectsFile());
    	}
    	catch(Exception e) {
    		throw new KettleException("Unable to save job details to the repository for job ["+jobMeta.getName()+"]", e);
    	}
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
