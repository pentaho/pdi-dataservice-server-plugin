package org.pentaho.di.repository.pur;

import java.util.Enumeration;
import java.util.ResourceBundle;

import javax.swing.JOptionPane;

import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.AbsSpoonPlugin;

import com.pentaho.commons.dsc.PentahoLicenseException;
import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.params.KParam;
import com.pentaho.commons.dsc.tlsup.util.ObfuscatedString;

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
      JOptionPane
          .showMessageDialog(
              null,
              messages
                  .getString(new ObfuscatedString(new long[] { 0x30E2E4A088DE4424L, 0x2D879C295D47AE8L,
                      0xF6CA345B0F2FBE8FL, 0xD6519406AA783413L, 0x6BFF6F99D967AA4AL, 0x4FF2F7EC3E1F3D76L,
                      0xD6E2BFFAACC4F634L, 0xD71C5E21ABE2A571L, 0xD0B595C3D287C676L }).toString() /* => "AbsSecurityCore.Error_0005_INVALID_OR_MISSING_PRODUCT_LICENSE" */),
              messages.getString(new ObfuscatedString(new long[] { 0x7C1A5C05F9D664C9L, 0x86938B18D4442883L,
                  0x50E90B1A4F710720L }).toString() /* => "Dialog.Error" */), JOptionPane.ERROR_MESSAGE);
      System.exit(1);
    }
  }

}
