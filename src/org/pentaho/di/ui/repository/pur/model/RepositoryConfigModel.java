/**
 * 
 */
package org.pentaho.di.ui.repository.pur.model;

import org.pentaho.ui.xul.XulEventSourceAdapter;

/**
 * @author rmansoor
 *
 */
public class RepositoryConfigModel extends XulEventSourceAdapter{

  private String url;
  private String name;
  private String description;
  private boolean modificationComments;

  /**
   * 
   */
  public RepositoryConfigModel() {
    // TODO Auto-generated constructor stub
  }
  
  public String getUrl() {
    return url;
  }
  public void setUrl(String url) {
    String previousVal = this.url;
    this.url = url;
    this.firePropertyChange("url", previousVal, url); //$NON-NLS-1$
    checkIfModelValid();
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    String previousVal = this.name;
    this.name = name;
    this.firePropertyChange("name", previousVal, name);   //$NON-NLS-1$
    checkIfModelValid();
  }
  public String getDescription() {
    return description;
  }
  public void setDescription(String description) {
    String previousVal = this.description;
    this.description = description;
    this.firePropertyChange("description", previousVal, description);//$NON-NLS-1$
  }
  public boolean isModificationComments() {
    return modificationComments;
  }
  public void setModificationComments(boolean modificationComments) {
    boolean previousVal = this.modificationComments;
    this.modificationComments = modificationComments;
    this.firePropertyChange("modificationComments", previousVal, modificationComments);//$NON-NLS-1$
  }
  public void clear() {
    setUrl("");
    setDescription("");
    setName("");
    setDescription("");
    setModificationComments(true);
  }
  public void checkIfModelValid() {
    this.firePropertyChange("valid", null, isValid());//$NON-NLS-1$
  }
  public boolean isValid() {
    return url != null && url.length() > 0 && name != null && name.length() > 0;
  }
}
