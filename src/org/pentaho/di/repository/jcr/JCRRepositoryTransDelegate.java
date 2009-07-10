package org.pentaho.di.repository.jcr;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.version.Version;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepLoaderException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.jcr.util.JCRObjectVersion;
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
import org.w3c.dom.Document;

public class JCRRepositoryTransDelegate extends JCRRepositoryBaseDelegate {
	
	private static Class<?> PKG = JCRRepository.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private static final String	TRANS_HOP_FROM	   = "TRANS_HOP_FROM";
	private static final String	TRANS_HOP_TO       = "TRANS_HOP_TO";
	private static final String	TRANS_HOP_ENABLED  = "TRANS_HOP_ENABLED";
	private static final String	TRANS_HOP_PREFIX   = "__TRANS_HOP__#";

	public JCRRepositoryTransDelegate(JCRRepository repository) {
		super(repository);
	}

	public void saveTrans(RepositoryElementInterface element, String versionComment, ProgressMonitorListener monitor) throws KettleException {
		try {
			TransMeta transMeta = (TransMeta)element;
			
			// We get the XML of the transformation.  However, we do NOT include any shared object nor step.
			// That way we can create a nice separation, preserve relationships, allow shared object rename, etc. 
			//
			String xml = transMeta.getXML(false, false, false, false, false);
			Node transNode = repository.saveAsXML(xml, element, versionComment, monitor, false);
			
			transNode.setProperty(JCRRepository.PROPERTY_NAME, element.getName());
			transNode.setProperty(JCRRepository.PROPERTY_DESCRIPTION, element.getDescription());
			
			// Remove all children before we continue.  We'll see how it versions...
			//
			NodeIterator childNodes = transNode.getNodes();
			while (childNodes.hasNext()) {
				Node childNode = childNodes.nextNode();
				childNode.remove();
			}
			
			// Now store the databases in the transformation.
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
			
			for (SlaveServer slaveServer : transMeta.getSlaveServers()) {
				if (slaveServer.hasChanged() || slaveServer.getObjectId()==null) {
					if (transMeta.isUsingSlaveServer(slaveServer)) {
						repository.saveAsXML(slaveServer.getXML(), slaveServer, versionComment, monitor, true);
					}
				}
			}

			for (ClusterSchema clusterSchema : transMeta.getClusterSchemas()) {
				if (clusterSchema.hasChanged() || clusterSchema.getObjectId()==null) {
					if (transMeta.isUsingClusterSchema(clusterSchema)) {
						repository.saveAsXML(clusterSchema.getXML(), clusterSchema, versionComment, monitor, true);
					}
				}
			}

			for (PartitionSchema partitionSchema : transMeta.getPartitionSchemas()) {
				if (partitionSchema.hasChanged() || partitionSchema.getObjectId()==null) {
					if (transMeta.isUsingPartitionSchema(partitionSchema)) {
						repository.saveAsXML(partitionSchema.getXML(), partitionSchema, versionComment, monitor, true);
					}
				}
			}
			
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
			
			for (StepMeta step : transMeta.getSteps()) {
				StepErrorMeta stepErrorMeta = step.getStepErrorMeta();
				if (stepErrorMeta!=null) {
					saveStepErrorMeta(stepErrorMeta, transMeta.getObjectId(), step.getObjectId());
				}
			}
			
			// Finally, save the hops
			//
			for (int i=0;i<transMeta.nrTransHops();i++) {
				TransHopMeta hop = transMeta.getTransHop(i);
				Node hopNode = transNode.addNode(TRANS_HOP_PREFIX+i, JCRRepository.NODE_TYPE_UNSTRUCTURED);
				
				hopNode.setProperty(TRANS_HOP_FROM, hop.getFromStep().getObjectId().getId());
				hopNode.setProperty(TRANS_HOP_TO, hop.getToStep().getObjectId().getId());
				hopNode.setProperty(TRANS_HOP_ENABLED, hop.getToStep().getObjectId().getId());
			}

			// Save the changes to all the steps
			//
            repository.getSession().save();
			Version version = transNode.checkin();
			transMeta.setObjectVersion(new JCRObjectVersion(version, versionComment, repository.getUserInfo().getLogin()));
		} catch(Exception e) {
			throw new KettleException("Unable to save transformation in the JCR repository", e);
		}
	}

	
	

	public TransMeta loadTransformation(String transname, RepositoryDirectory repdir, ProgressMonitorListener monitor, boolean setInternalVariables, String versionLabel) throws KettleException {

		String path = repository.calcRelativeNodePath(repdir, transname, JCRRepository.EXT_TRANSFORMATION); 
		
		try {
			Node node = repository.getRootNode().getNode(path);
			Version version = repository.getVersion(node, versionLabel);
			Node transNode = repository.getVersionNode(version);
			
			Property xmlProp = transNode.getProperty(JCRRepository.PROPERTY_XML);
			
			Document doc = XMLHandler.loadXMLString(xmlProp.getString());
			TransMeta transMeta = new TransMeta( XMLHandler.getSubNode(doc, TransMeta.XML_TAG), repository);

			// Grab the Version comment...
			//
			transMeta.setObjectVersion( repository.getObjectVersion(version) );

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
	                
	                // Get the cluster schema name
	                stepMeta.setClusterSchemaName( repository.getStepAttributeString(stepMeta.getObjectId(), "cluster_schema") );
	                
	                transMeta.addStep(stepMeta);
				}
			}
			
			// Read the hops...
			//
			nodes = transNode.getNodes();
			while (nodes.hasNext()) {
				Node hopNode = nodes.nextNode();
				String name = hopNode.getName();
				if (!name.endsWith(JCRRepository.EXT_STEP) && name.startsWith(TRANS_HOP_PREFIX)) {
					String stepFromId = repository.getPropertyString( hopNode, TRANS_HOP_FROM);
					String stepToId = repository.getPropertyString( hopNode, TRANS_HOP_TO);
					boolean enabled = repository.getPropertyBoolean( hopNode, TRANS_HOP_ENABLED, true);
					
					StepMeta stepFrom = StepMeta.findStep(transMeta.getSteps(), new StringObjectId(stepFromId));
					StepMeta stepTo = StepMeta.findStep(transMeta.getSteps(), new StringObjectId(stepToId));
					
					transMeta.addTransHop( new TransHopMeta(stepFrom, stepTo, enabled) );
				}

			}
			return transMeta;
		}
		catch(Exception e) {
			throw new KettleException("Unable to load transformation from JCR workspace path ["+path+"]", e);
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
    		if (meta.getPartitionSchema()!=null) {
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
			repository.deleteObject(transformationId);
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
}
