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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderInterface;
import org.pentaho.di.core.exception.KettleException;

/**
 * Test cases for encryption, to make sure that encrypted password remain the same between versions.
 * 
 * @author Matt Casters
 */
public class AESTwoWayPasswordEncoderTest {

  @BeforeClass
  public static void setupBeforeClass() throws KettleException {
    KettleClientEnvironment.init();
  }

  private TwoWayPasswordEncoderInterface getEncoder() throws KettleException {
    AESTwoWayPasswordEncoder encoder = new AESTwoWayPasswordEncoder();
    encoder.init( "MySecretKey12345" );
    return encoder;
  }

  /**
   * Test password encryption.
   * 
   * @throws KettleException
   */
  @Test
  public void testEncode1() throws KettleException {

    TwoWayPasswordEncoderInterface encoder = getEncoder();

    String encryption;

    encryption = encoder.encode( null, false );
    assertTrue( "".equals( encryption ) );

    encryption = encoder.encode( "" );
    assertTrue( "".equals( encryption ) );

    encryption = encoder.encode( "     ", false );
    assertTrue( "B+Zvu6Gf93b3gurygLk4RA==".equals( encryption ) );

    encryption = encoder.encode( "Test of different encryptions!!@#$%", false );
    assertTrue( "q0hdNEYmBVAWa5w2HP2MvvG9nVyi9LzPxAHPwKoLD8IazQLzF6E+8toxbj7SYECw".equals( encryption ) );

    encryption = encoder.encode( "  Spaces left", false );
    assertTrue( "bytxsukId2STlJkloobm/w==".equals( encryption ) );

    encryption = encoder.encode( "Spaces right", false );
    assertTrue( "q5JJxX40QGFNvt8iBcBa5w==".equals( encryption ) );

    encryption = encoder.encode( "     Spaces  ", false );
    assertTrue( "n2VhpqC9hyYciT0ra/ohsg==".equals( encryption ) );

    encryption = encoder.encode( "1234567890", false );
    assertTrue( "DlPr28hG3zF1sc7b0Oabcw==".equals( encryption ) );
  }

  /**
   * Test password decryption.
   * 
   * @throws KettleException
   */
  @Test
  public void testDecode1() throws KettleException {
    TwoWayPasswordEncoderInterface encoder = getEncoder();
    String encryption;
    String decryption;

    assertNotNull( encoder );

    encryption = encoder.encode( null );
    decryption = encoder.decode( encryption );
    assertTrue( "".equals( decryption ) );

    encryption = encoder.encode( "" );
    decryption = encoder.decode( encryption );
    assertTrue( "".equals( decryption ) );

    encryption = encoder.encode( "     " );
    decryption = encoder.decode( encryption );
    assertTrue( "     ".equals( decryption ) );

    encryption = encoder.encode( "Test of different encryptions!!@#$%" );
    decryption = encoder.decode( encryption );
    assertTrue( "Test of different encryptions!!@#$%".equals( decryption ) );

    encryption = encoder.encode( "  Spaces left" );
    decryption = encoder.decode( encryption );
    assertTrue( "  Spaces left".equals( decryption ) );

    encryption = encoder.encode( "Spaces right" );
    decryption = encoder.decode( encryption );
    assertTrue( "Spaces right".equals( decryption ) );

    encryption = encoder.encode( "     Spaces  " );
    decryption = encoder.decode( encryption );
    assertTrue( "     Spaces  ".equals( decryption ) );

    encryption = encoder.encode( "1234567890" );
    decryption = encoder.decode( encryption );
    assertTrue( "1234567890".equals( decryption ) );
  }

  /**
   * Test password encryption (variable style).
   * 
   * @throws KettleException
   */
  @Test
  public void testEncode2() throws KettleException {
    TwoWayPasswordEncoderInterface encoder = getEncoder();

    String encryption;

    encryption = encoder.encode( null, true );
    assertTrue( "".equals( encryption ) );

    encryption = encoder.encode( "", true );
    assertTrue( "".equals( encryption ) );

    encryption = encoder.encode( "String", true );
    assertTrue( "AES 8GleDNwnTD1/DuLhrtAgqw==".equals( encryption ) );

    encryption = encoder.encode( " ${VAR} String", true );
    assertTrue( " ${VAR} String".equals( encryption ) );

    encryption = encoder.encode( " %%VAR%% String", true );
    assertTrue( " %%VAR%% String".equals( encryption ) );

    encryption = encoder.encode( " %% VAR String", true );
    assertTrue( "AES gnuFYYwa4o2t1dOuWSJZWA==".equals( encryption ) );

    encryption = encoder.encode( "${%%$$$$", true );
    assertTrue( "AES ejbLp5BawwnjqsQfGtGYuA==".equals( encryption ) );
  }

  /**
   * Test password decryption (variable style).
   * 
   * @throws KettleException
   */
  @Test
  public void testDecode2() throws KettleException {
    TwoWayPasswordEncoderInterface encoder = getEncoder();

    String encryption;
    String decryption;

    encryption = encoder.encode( null );
    decryption = encoder.decode( encryption );
    assertTrue( "".equals( decryption ) );

    encryption = encoder.encode( "" );
    decryption = encoder.decode( encryption );
    assertTrue( "".equals( decryption ) );

    encryption = encoder.encode( "String" );
    decryption = encoder.decode( encryption );
    assertTrue( "String".equals( decryption ) );

    encryption = encoder.encode( " ${VAR} String", true );
    decryption = encoder.decode( encryption, true );
    assertTrue( " ${VAR} String".equals( decryption ) );

    encryption = encoder.encode( " %%VAR%% String", true );
    decryption = encoder.decode( encryption, true );
    assertTrue( " %%VAR%% String".equals( decryption ) );

    encryption = encoder.encode( " %% VAR String", true );
    decryption = encoder.decode( encryption, true );
    assertTrue( " %% VAR String".equals( decryption ) );

    encryption = encoder.encode( "${%%$$$$", true );
    decryption = encoder.decode( encryption, true );
    assertTrue( "${%%$$$$".equals( decryption ) );
  }
}
