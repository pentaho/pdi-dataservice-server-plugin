package org.pentaho.di.trans.dataservice.serialization;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
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
  @InjectMocks TransOpenedExtensionPointPlugin extensionPointPlugin;
  @Mock TransMeta transMeta;

  @Test
  public void testCallExtensionPoint() throws Exception {
    extensionPointPlugin.callExtensionPoint( mock( LogChannel.class ), transMeta );

    verify( transMeta ).addStepChangeListener( service );
    verify( transMeta ).addContentChangedListener( service );
  }
}
