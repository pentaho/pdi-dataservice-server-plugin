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

package com.pentaho.di.core.encryption;

import java.util.ArrayList;
import java.util.List;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.vfs.FileObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.KettleTwoWayPasswordEncoder;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderInterface;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPlugin;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.vfs.KettleVFS;

@TwoWayPasswordEncoderPlugin( id = "AES", name = "AES Password Encoder",
    description = "Encrypts and decrypts passwords using AES" )
public class AESTwoWayPasswordEncoder implements TwoWayPasswordEncoderInterface {

  public static final String DETAIL_AES_SECRET_KEY = "secret_key";
  private Cipher cipher;
  private byte[] aesKey;
  private SecretKeySpec secretKey;

  public static final String AES_PREFIX = "AES ";
  public static final String KETTLE_AES_KEY_FILE = "KETTLE_AES_KEY_FILE";
  public static final String KETTLE_AES_KETTLE_PASSWORD_HANDLING = "KETTLE_AES_KETTLE_PASSWORD_HANDLING";
  public static final String KETTLE_AES_KETTLE_PASSWORD_HANDLING_DECODE = "DECODE";

  private KettleTwoWayPasswordEncoder kettleEncoder = null;
  private boolean decodeKettlePasswords;

  public AESTwoWayPasswordEncoder() throws RuntimeException {
  }

  /* (non-Javadoc)
   * @see org.pentaho.di.core.encryption.TwoWayPasswordEncoderInterface#init()
   */
  public void init() throws KettleException {

    String keyFile = EnvUtil.getSystemProperty( KETTLE_AES_KEY_FILE );
    if ( keyFile == null ) {
      throw new KettleException( "System variable " + KETTLE_AES_KEY_FILE + " is not defined." );
    }

    try {
      FileObject fileObject = KettleVFS.getFileObject( keyFile );
      if ( !fileObject.exists() ) {
        throw new KettleException(
            "Unable to find file specified by variable " + KETTLE_AES_KEY_FILE + " : " + keyFile );
      }

      String keyString = KettleVFS.getTextFileContent( keyFile, Const.XML_ENCODING );

      // Remove possible CR/LF and leading/trailing spaces.
      //
      keyString = Const.trim( keyString );

      aesKey = keyString.getBytes( Const.XML_ENCODING );

      initSecretKey();
    } catch ( Exception e ) {
      throw new KettleException( "Unable to initialize AES encoder", e );
    }

    configureDecodeKettlePasswords();
  }

  protected void configureDecodeKettlePasswords() {
    // See if we need to try to decode Kettle encoded passwords...
    //
    kettleEncoder = new KettleTwoWayPasswordEncoder();
    String kettlePasswordHandling =
        Const.NVL( EnvUtil.getSystemProperty( KETTLE_AES_KETTLE_PASSWORD_HANDLING ),
            KETTLE_AES_KETTLE_PASSWORD_HANDLING_DECODE );

    // If the variable is set to anything but "DECODE", a runtime exception will be thrown...
    //
    decodeKettlePasswords = KETTLE_AES_KETTLE_PASSWORD_HANDLING_DECODE.equalsIgnoreCase( kettlePasswordHandling );
  }

  public void init( String aesKeyString ) throws KettleException {
    try {
      aesKey = aesKeyString.getBytes( Const.XML_ENCODING );
      initSecretKey();
    } catch ( Exception e ) {
      throw new KettleException( "Unable to initialize AES encoder", e );
    }
    configureDecodeKettlePasswords();
  }

  private void initSecretKey() throws KettleException {
    try {
      cipher = Cipher.getInstance( "AES/ECB/PKCS5Padding" );

      secretKey = new SecretKeySpec( aesKey, "AES" );
    } catch ( Exception e ) {
      throw new KettleException( "Unable to initialize AES encoder", e );
    }
  }

  @Override
  public String encode( String password ) {
    if ( Const.isEmpty( password ) ) {
      return "";
    }

    try {
      synchronized ( cipher ) {
        cipher.init( Cipher.ENCRYPT_MODE, secretKey );
        byte[] encryptedBinary = cipher.doFinal( password.getBytes( Const.XML_ENCODING ) );
        String encryptedString = new String( Base64.encodeBase64( encryptedBinary ), Const.XML_ENCODING );
        return encryptedString;
      }
    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to AES encrypt password", e );
    }
  }

  @Override
  public String encode( String password, boolean includePrefix ) {
    if ( Const.isEmpty( password ) ) {
      return "";
    }
    List<String> varList = new ArrayList<String>();
    StringUtil.getUsedVariables( password, varList, true );
    if ( !varList.isEmpty() ) {
      // The password contains variables so we just give back the input.
      // We consider the password to be configured with a variable
      //
      return password;
    }

    if ( includePrefix ) {
      return AES_PREFIX + encode( password );
    } else {
      return encode( password );
    }
  }

  @Override
  public String decode( String encodedPassword, boolean optionallyEncrypted ) {
    if ( optionallyEncrypted ) {

      // Check to see if we need to throw a runtime exception for using a Kettle encoded password...
      //
      String kettlePrefix = kettleEncoder.getPrefixes()[0];
      if ( !Const.isEmpty( encodedPassword ) && encodedPassword.startsWith( kettlePrefix ) ) {
        if ( decodeKettlePasswords ) {
          return kettleEncoder.decode( encodedPassword, optionallyEncrypted );
        } else {
          throw new RuntimeException( "A Kettle encoded password was used: '" + encodedPassword + "'" );
        }
      }

      if ( !Const.isEmpty( encodedPassword ) && encodedPassword.startsWith( AES_PREFIX ) ) {
        return decode( encodedPassword.substring( AES_PREFIX.length() ) );
      } else {
        return encodedPassword;
      }
    } else {
      return decode( encodedPassword );
    }
  }

  @Override
  public String decode( String encodedPassword ) {
    if ( Const.isEmpty( encodedPassword ) ) {
      return "";
    }
    try {
      synchronized ( cipher ) {
        cipher.init( Cipher.DECRYPT_MODE, secretKey );
        byte[] passwordBinary = Base64.decodeBase64( encodedPassword.getBytes( Const.XML_ENCODING ) );
        byte[] encryptedBinary = cipher.doFinal( passwordBinary );
        String encryptedString = new String( encryptedBinary, Const.XML_ENCODING );
        return encryptedString;
      }
    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to AES decrypt password", e );
    }
  }

  @Override
  public String[] getPrefixes() {
    return new String[] { AES_PREFIX };
  }

}
