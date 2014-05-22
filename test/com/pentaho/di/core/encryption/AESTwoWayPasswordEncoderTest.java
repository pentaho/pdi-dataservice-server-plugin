/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.core.encryption;

import junit.framework.TestCase;

import org.pentaho.di.core.exception.KettleException;

/**
 * Test cases for encryption, to make sure that encrypted password remain the same between versions.
 *
 * @author Matt Casters
 */
public class AESTwoWayPasswordEncoderTest extends TestCase {

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
  public void testDecode1() throws KettleException {
    TwoWayPasswordEncoderInterface encoder = getEncoder();

    String encryption;
    String decryption;

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
