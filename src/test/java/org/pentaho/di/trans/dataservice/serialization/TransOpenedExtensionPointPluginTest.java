package org.pentaho.di.trans.dataservice.serialization;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.trans.TransMeta;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class TransOpenedExtensionPointPluginTest {

  @Mock SynchronizationService service;
  @Mock TransMeta transMeta;

  TransOpenedExtensionPointPlugin extensionPointPlugin;

  @Before
  public void setUp() throws Exception {
    extensionPointPlugin = new TransOpenedExtensionPointPlugin( service );
  }

  @Test
  public void testCallExtensionPoint() throws Exception {
    extensionPointPlugin.callExtensionPoint( mock( LogChannel.class ), transMeta );

    verify( service ).install( transMeta );
  }
}
