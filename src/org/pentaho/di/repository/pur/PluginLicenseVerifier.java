/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur;

import com.pentaho.commons.dsc.PentahoLicenseException;
import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.params.KParam;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.repository.EESpoonPlugin;

import java.util.Enumeration;
import java.util.ResourceBundle;

public class PluginLicenseVerifier implements java.io.Serializable {

  private static final long serialVersionUID = 7324261459822440599L; /* EESOURCE: UPDATE SERIALVERUID */
  private static final Log logger = LogFactory.getLog(PluginLicenseVerifier.class);

  private static ResourceBundle messages = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(EESpoonPlugin.class, key);
    }

  };

  public static void verify() {
    try {
      PentahoLicenseVerifier.verify(new KParam(PurRepositoryMeta.BUNDLE_REF_NAME));
    } catch (PentahoLicenseException ple) {
      // simply log the license verification failure
      getLogger().error(
          BaseMessages.getString(PluginLicenseVerifier.class,
              "PluginLicenseVerifier.LicenseValidationFailed.Log")); //$NON-NLS-1$
    }
  }

  public static Log getLogger() {
    return logger;
  }
}
