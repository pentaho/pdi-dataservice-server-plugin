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
    IUnifiedRepository mockRepo = mock( IUnifiedRepository.class );
    RepositoryConnectResult result = mock( RepositoryConnectResult.class );
    when( result.getUnifiedRepository() ).thenReturn( mockRepo );
    IRepositoryConnector connector = mock( IRepositoryConnector.class );
    when( connector.connect( anyString(), anyString() ) ).thenReturn( result );
    PurRepositoryMeta mockMeta = mock( PurRepositoryMeta.class );
    purRepository.init( mockMeta );
    purRepository.setPurRepositoryConnector( connector );
    // purRepository.setTest( mockRepo );
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
    when( mockFile.getId() ).thenReturn( testFileId );
    when( mockRepo.getTree( anyString(), anyInt(), anyString(), anyBoolean() ) ).thenReturn( mockRepositoryTree );
    when( mockRepositoryTree.getFile() ).thenReturn( mockRootFolder );
    when( mockRootFolder.getId() ).thenReturn( "/" );
    purRepository.connect( "TEST_USER", "TEST_PASSWORD" );
    purRepository.getObjectInformation( objectId, repositoryObjectType );
    verify( mockRepo ).getAcl( testFileId );
  }
}
