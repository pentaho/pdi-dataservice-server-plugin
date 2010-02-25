package org.pentaho.di.repository.pur;

import java.util.List;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.BaseRepositoryMeta;
import org.pentaho.di.repository.RepositoryCapabilities;
import org.pentaho.di.repository.RepositoryMeta;
import org.w3c.dom.Node;

import com.pentaho.commons.dsc.PentahoDscContent;
import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.params.KParam;

public class PurRepositoryMeta extends BaseRepositoryMeta implements RepositoryMeta {

	/** The id as specified in the repository plugin meta, used for backward compatibility only */
	public static String REPOSITORY_TYPE_ID = "KettleJCRRepository";
	
	private PurRepositoryLocation repositoryLocation;

	private boolean	versionCommentMandatory;

	public PurRepositoryMeta() {
		super(REPOSITORY_TYPE_ID);
    PluginLicenseVerifier.verify();
	}
	
	public PurRepositoryMeta(String id, String name, String description, PurRepositoryLocation repositoryLocation, boolean versionCommentMandatory)
	{
		super(id, name, description);
    PluginLicenseVerifier.verify();
		this.repositoryLocation = repositoryLocation;
		this.versionCommentMandatory = versionCommentMandatory;
	}
	
	public String getXML()
	{
    StringBuffer retval = new StringBuffer(100);
		
		retval.append("  ").append(XMLHandler.openTag(XML_TAG));
		retval.append(super.getXML());
		retval.append("    ").append(XMLHandler.addTagValue("repository_location_url",  repositoryLocation.getUrl()));
		retval.append("    ").append(XMLHandler.addTagValue("version_comment_mandatory",  versionCommentMandatory));
		retval.append("  ").append(XMLHandler.closeTag(XML_TAG));
        
		return retval.toString();
	}
	
	public void loadXML(Node repnode, List<DatabaseMeta> databases) throws KettleException
	{
		super.loadXML(repnode, databases);
		try
		{
			String url = XMLHandler.getTagValue(repnode, "repository_location_url") ;
			this.repositoryLocation = new PurRepositoryLocation(url);
			this.versionCommentMandatory = "Y".equalsIgnoreCase(XMLHandler.getTagValue(repnode, "version_comment_mandatory")) ;
		}
		catch(Exception e)
		{
			throw new KettleException("Unable to load Kettle database repository meta object", e);
		}
	}	


	public RepositoryCapabilities getRepositoryCapabilities() {
	  
    PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());
    if (dscContent.getSubject()==null){
      return null;
    }
  	return new RepositoryCapabilities() {
  		public boolean supportsUsers() { return true; }
  		public boolean managesUsers() { return true; }
  		public boolean isReadOnly() { return false; }
  		public boolean supportsRevisions() { return true; }
  		public boolean supportsMetadata() { return true; }
  		public boolean supportsLocking() { return true; }
  		public boolean hasVersionRegistry() { return true; }
      public boolean supportsAcls() { return true;}
  	};
	}

	/**
	 * @return the repositoryLocation
	 */
	public PurRepositoryLocation getRepositoryLocation() {
		return repositoryLocation;
	}

	/**
	 * @param repositoryLocation the repositoryLocation to set
	 */
	public void setRepositoryLocation(PurRepositoryLocation repositoryLocation) {
		this.repositoryLocation = repositoryLocation;
	}

	public boolean isVersionCommentMandatory() {
		return versionCommentMandatory;
	}

	public void setVersionCommentMandatory(boolean versionCommentMandatory) {
		this.versionCommentMandatory = versionCommentMandatory;
	}
}
