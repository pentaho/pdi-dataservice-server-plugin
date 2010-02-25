package org.pentaho.di.repository.pur;

import java.util.Enumeration;
import java.util.ResourceBundle;

import javax.swing.JOptionPane;

import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.AbsSpoonPlugin;

import com.pentaho.commons.dsc.PentahoLicenseException;
import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.params.KParam;

public class PluginLicenseVerifier {

  private static ResourceBundle messages = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(AbsSpoonPlugin.class, key);
    }

  };

  public static void verify() {
    try {
      PentahoLicenseVerifier.verify(new KParam());
    } catch (PentahoLicenseException ple) {
      ple.printStackTrace();
      JOptionPane.showMessageDialog(null, messages.getString("AbsSecurityCore.Error_0005_INVALID_OR_MISSING_PRODUCT_LICENSE"), messages
          .getString("Dialog.Error"), JOptionPane.ERROR_MESSAGE);
      System.exit(1);
    }
  }

}
