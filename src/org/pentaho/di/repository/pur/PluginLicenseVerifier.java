/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
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

  protected static ResourceBundle messages = new ResourceBundle() {

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
