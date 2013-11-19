package org.pentaho.di.repository.pur;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.repository2.unified.RepositoryFileTree;

public class PurRepositoryUnitTest {
  @Test
  public void testGetObjectInformationGetsAclByFileId() throws KettleException {
    PurRepository purRepository = new PurRepository();
    PurRepositoryMeta mockMeta = mock( PurRepositoryMeta.class );
    purRepository.init( mockMeta );
    IUnifiedRepository mockRepo = mock( IUnifiedRepository.class );
    purRepository.setTest( mockRepo );
    ObjectId objectId = mock( ObjectId.class );
    RepositoryFile mockFile = mock( RepositoryFile.class );
    RepositoryFile mockRootFolder = mock( RepositoryFile.class );
    RepositoryObjectType repositoryObjectType = RepositoryObjectType.TRANSFORMATION;
    RepositoryFileTree mockRepositoryTree = mock( RepositoryFileTree.class );
    String testId = "TEST_ID";
    String testFileId = "TEST_FILE_ID";
    when( objectId.getId() ).thenReturn( testId );
    when( mockRepo.getFileById( testId ) ).thenReturn( mockFile );
    when( mockFile.getPath() ).thenReturn( "/home/testuser/path.ktr" );
    when (mockFile.getId()).thenReturn( testFileId );
    when( mockRepo.getTree( anyString(), anyInt(), anyString(), anyBoolean() ) ).thenReturn( mockRepositoryTree );
    when( mockRepositoryTree.getFile() ).thenReturn( mockRootFolder );
    when( mockRootFolder.getId() ).thenReturn( "/" );
    purRepository.getObjectInformation( objectId, repositoryObjectType );
    verify( mockRepo ).getAcl( testFileId );
  }
}
