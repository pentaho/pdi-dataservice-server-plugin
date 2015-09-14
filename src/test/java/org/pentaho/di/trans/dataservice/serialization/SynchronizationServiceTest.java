package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class SynchronizationServiceTest {

  @Mock TransMeta transMeta;
  @Mock DataServiceDelegate delegate;
  @Mock LogChannel logChannel;
  @Mock DataServiceMeta dataServiceMeta;
  @InjectMocks SynchronizationService service;

  @Captor ArgumentCaptor<Function<? super Exception, ?>> errorHandler;

  @Before
  public void setUp() throws Exception {
    when( delegate.getLogChannel() ).thenReturn( logChannel );
  }

  @Test
  public void testContentSafe() throws Exception {
    service.contentSafe( transMeta );

    verify( delegate ).sync( same( transMeta ), errorHandler.capture() );

    UndefinedDataServiceException undefinedException = new UndefinedDataServiceException( dataServiceMeta );
    when( delegate.showPrompt( anyString(), anyString() ) ).thenReturn( false, true );
    errorHandler.getValue().apply( undefinedException );
    verify( delegate, never() ).removeDataService( dataServiceMeta, false );
    errorHandler.getValue().apply( undefinedException );
    verify( delegate ).removeDataService( dataServiceMeta, false );
    verify( logChannel, times( 2 ) ).logError( anyString(), same( undefinedException ) );

    DataServiceAlreadyExistsException conflictException = new DataServiceAlreadyExistsException( dataServiceMeta );
    errorHandler.getValue().apply( conflictException );
    verify( delegate ).suggestEdit( same( dataServiceMeta ), anyString(), anyString() );
    verify( logChannel ).logError( anyString(), same( conflictException ) );

    MetaStoreException metaStoreException = new MetaStoreException();
    errorHandler.getValue().apply( metaStoreException );
    verify( logChannel ).logError( anyString(), same( metaStoreException ) );
  }

  @Test
  public void testOnStepChange() throws Exception {
    StepMeta oldMeta = mock( StepMeta.class );
    StepMeta newMeta = mock( StepMeta.class );

    when( oldMeta.getName() ).thenReturn( "ORIGINAL STEP" );
    when( newMeta.getName() ).thenReturn( "ORIGINAL STEP" );
    service.onStepChange( transMeta, oldMeta, newMeta );

    when( newMeta.getName() ).thenReturn( "DIFFERENT STEP" );
    service.onStepChange( transMeta, oldMeta, newMeta );

    verifyNoMoreInteractions( dataServiceMeta );

    when( delegate.getDataServiceByStepName( transMeta, "ORIGINAL STEP" ) )
      .thenReturn( dataServiceMeta );

    service.onStepChange( transMeta, oldMeta, newMeta );
    verify( dataServiceMeta ).setStepname( "DIFFERENT STEP" );
    verify( delegate ).save( dataServiceMeta );
  }
}
