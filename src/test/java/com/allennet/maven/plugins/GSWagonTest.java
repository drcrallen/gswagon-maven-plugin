/*
 * Copyright  2017 Charles Allen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.allennet.maven.plugins;

import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.api.client.testing.http.apache.MockHttpClient;
import com.google.cloud.HttpTransportOptions;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.http.client.HttpClient;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.repository.Repository;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.CustomMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import static com.allennet.maven.plugins.GSWagon.PROJECT_ID_PROPERTY_PREFIX;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.strictMock;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GSWagonTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final BlobId BLOB_ID = BlobId.of("bucket", "/key");
  private final String repoId = "id";
  private final ConnectionPOJO connectionPOJO = createMockConnectionPOJO();
  private final GSWagon gsWagon = new GSWagon()
  {
    HttpClient buildClient()
    {
      return connectionPOJO.client;
    }

    Storage buildStorage()
    {
      return connectionPOJO.storage;
    }

    public Repository getRepository()
    {
      return new Repository(repoId, "gs://bucket/key");
    }
  };
  private String priorId = null;

  @Before
  public void setUp()
  {
    final String propertyString = gsWagon.getPropertyString(repoId);
    priorId = System.getProperty(propertyString);
    System.setProperty(propertyString, "project_id");
  }

  @Test
  public void testBuildTransportOptions()
  {
    gsWagon.swapAndCloseConnection(connectionPOJO);
    final HttpTransportOptions transportOptions = (HttpTransportOptions) gsWagon.buildTransportOptions(connectionPOJO.client);
    assertEquals(ApacheHttpTransport.class, transportOptions.getHttpTransportFactory().create().getClass());
    assertEquals(gsWagon.getTimeout(), transportOptions.getConnectTimeout());
    assertEquals(gsWagon.getReadTimeout(), transportOptions.getReadTimeout());
    assertEquals(
        connectionPOJO.client,
        ((ApacheHttpTransport) transportOptions.getHttpTransportFactory().create()).getHttpClient()
    );
  }

  @Test
  public void testRetrySettings()
  {
    assertNotNull(gsWagon.buildRetrySettings());
  }

  @Test
  public void testToBlobID()
  {
    final String resource = "resource";
    final GSWagon gsWagon = new GSWagon();
    gsWagon.swapAndCloseConnection(connectionPOJO);
    final BlobId blobId = gsWagon.toBlobID(resource);
    assertEquals(connectionPOJO.baseId.getBucket(), blobId.getBucket());
    assertNull(connectionPOJO.baseId.getGeneration());
    assertEquals(String.format("%s/%s", connectionPOJO.baseId.getName(), resource), blobId.getName());
  }

  @Test
  public void testOpenConnectionInternal() throws Exception
  {
    gsWagon.openConnectionInternal();
    final BlobId baseId = gsWagon.getBaseId();
    assertEquals("bucket", baseId.getBucket());
    assertEquals("key", baseId.getName());
    assertNull(baseId.getGeneration());
  }

  @Test
  public void testOpenRepeatedClosesOld() throws Exception
  {
    gsWagon.openConnectionInternal();
    gsWagon.openConnectionInternal();
  }

  @Test
  public void testRepeatedClose() throws Exception
  {
    gsWagon.closeConnection();
    gsWagon.closeConnection();
  }

  @Test
  public void testOpenCloseConnection() throws Exception
  {
    gsWagon.openConnectionInternal();
    gsWagon.closeConnection();
  }

  @Test
  public void testSwapAndCloseConnection()
  {
    final GSWagon gsWagon = new GSWagon()
    {
      @Override
      HttpClient buildClient()
      {
        return connectionPOJO.client;
      }
    };
    gsWagon.swapAndCloseConnection(connectionPOJO);
    assertEquals(connectionPOJO.client, gsWagon.buildClient());
    assertEquals(connectionPOJO.baseId, gsWagon.getBaseId());
    assertEquals(connectionPOJO.storage, gsWagon.getStorage());
  }

  @Test
  public void testBadRepo() throws Exception
  {
    expectedException.expect(new CustomMatcher<Object>(IllegalArgumentException.class.getName())
    {
      @Override
      public boolean matches(Object item)
      {
        return item instanceof IllegalArgumentException;
      }
    });
    new GSWagon()
    {
      public Repository getRepository()
      {
        return new Repository("id", "http://bucket/key");
      }
    }.openConnectionInternal();
  }

  @Test
  public void testGetItem() throws Exception
  {
    final HttpClient client = strictMock(HttpClient.class);
    final Storage storage = createStrictMock(Storage.class);

    final GSWagon gsWagon = new GSWagon()
    {
      @Override
      void get(Blob blob, File file) throws IOException, TransferFailedException
      {
        // noop
      }
    };

    gsWagon.swapAndCloseConnection(new ConnectionPOJO(
        storage,
        BLOB_ID,
        client
    ));

    final Capture<BlobId> blobIdCapture = Capture.newInstance();
    expect(storage.get(capture(blobIdCapture))).andReturn(createStrictMock(Blob.class)).once();
    replay(storage);
    final File outFile = temporaryFolder.newFile();
    gsWagon.get("artifact", outFile);
    verify(storage);
    assertTrue(blobIdCapture.hasCaptured());
    assertEquals(connectionPOJO.baseId.getBucket(), blobIdCapture.getValue().getBucket());
    assertEquals(
        String.format("%s/%s", connectionPOJO.baseId.getName(), "artifact"),
        blobIdCapture.getValue().getName()
    );
    assertNull(blobIdCapture.getValue().getGeneration());
  }

  @Test
  public void testGetItemNotFound() throws Exception
  {
    expectedException.expect(new CustomMatcher<Object>(ResourceDoesNotExistException.class.getName())
    {
      @Override
      public boolean matches(Object item)
      {
        return item instanceof ResourceDoesNotExistException;
      }
    });
    final HttpClient client = strictMock(HttpClient.class);
    final Storage storage = createStrictMock(Storage.class);
    final GSWagon gsWagon = new GSWagon()
    {
      @Override
      void get(Blob blob, File file) throws IOException, TransferFailedException
      {
        // noop
      }
    };

    gsWagon.swapAndCloseConnection(new ConnectionPOJO(
        storage,
        BLOB_ID,
        client
    ));
    expect(storage.get(EasyMock.<BlobId>anyObject())).andReturn(null).once();
    replay(storage);
    final File outFile = temporaryFolder.newFile();
    gsWagon.get("artifact", outFile);
  }

  @Test
  public void testGetBlob() throws Exception
  {
    final long size = 100;
    final Blob blob = createStrictMock(Blob.class);
    final ReadChannel readChannel = createStrictMock(ReadChannel.class);
    expect(blob.getName()).andReturn(connectionPOJO.baseId.getName() + "/foo").once();
    expect(blob.getSize()).andReturn(size).once();
    expect(blob.reader()).andReturn(readChannel).once();
    expect(readChannel.read(anyObject()))
        .andReturn(0).times(10)
        .andReturn(1).times((int) size);
    readChannel.close();
    expectLastCall().once();
    replay(readChannel, blob);
    final File outFile = temporaryFolder.newFile();
    outFile.delete();
    assertFalse(outFile.exists());
    gsWagon.swapAndCloseConnection(connectionPOJO);
    gsWagon.get(blob, outFile);
    assertTrue(outFile.exists());
    verify(readChannel, blob);
  }

  @Test
  public void testGetBlobTransferFailedException() throws Exception
  {
    expectedException.expect(TransferFailedException.class);
    final long size = 100;
    final Blob blob = createStrictMock(Blob.class);
    final ReadChannel readChannel = createStrictMock(ReadChannel.class);
    expect(blob.getName()).andReturn(connectionPOJO.baseId.getName() + "/foo").once();
    expect(blob.getSize()).andReturn(size).once();
    expect(blob.reader()).andReturn(readChannel).once();
    expect(readChannel.read(anyObject()))
        .andReturn(-1).once();
    readChannel.close();
    expectLastCall().once();
    replay(readChannel, blob);
    final File outFile = temporaryFolder.newFile();
    outFile.delete();
    assertFalse(outFile.exists());
    gsWagon.swapAndCloseConnection(connectionPOJO);
    gsWagon.get(blob, outFile);
  }

  @Test
  public void testGetBlobIOException() throws Exception
  {
    expectedException.expect(IOException.class);
    final long size = 100;
    final Blob blob = createStrictMock(Blob.class);
    final ReadChannel readChannel = createStrictMock(ReadChannel.class);
    expect(blob.getName()).andReturn(connectionPOJO.baseId.getName() + "/foo").once();
    expect(blob.getSize()).andReturn(size).once();
    expect(blob.reader()).andReturn(readChannel).once();
    expect(readChannel.read(anyObject()))
        .andThrow(new IOException("test")).once();
    readChannel.close();
    expectLastCall().once();
    replay(readChannel, blob);
    final File outFile = temporaryFolder.newFile();
    outFile.delete();
    assertFalse(outFile.exists());
    gsWagon.swapAndCloseConnection(connectionPOJO);
    gsWagon.get(blob, outFile);
  }

  @Test
  public void testGetBlobErasesOld() throws Exception
  {
    final long size = 100;
    final Blob blob = createStrictMock(Blob.class);
    final ReadChannel readChannel = createStrictMock(ReadChannel.class);
    expect(blob.getName()).andReturn(connectionPOJO.baseId.getName() + "/foo").once();
    expect(blob.getSize()).andReturn(size).once();
    expect(blob.reader()).andReturn(readChannel).once();
    expect(readChannel.read(anyObject()))
        .andReturn(0).times(10)
        .andReturn(1).times((int) size);
    readChannel.close();
    expectLastCall().once();
    replay(readChannel, blob);
    final File outFile = temporaryFolder.newFile();
    outFile.delete();
    assertFalse(outFile.exists());
    try (final RandomAccessFile raf = new RandomAccessFile(outFile, "rw")) {
      raf.setLength(size * 10);
    }
    assertTrue(outFile.exists());
    assertEquals(size * 10, outFile.length());
    gsWagon.swapAndCloseConnection(connectionPOJO);
    gsWagon.get(blob, outFile);
    assertTrue(outFile.exists());
    verify(readChannel, blob);
  }

  @Test
  public void testGetIfNewer() throws Exception
  {
    final long time = 13489731789L;
    final GSWagon gsWagon = new GSWagon()
    {
      void get(Blob blob, File file) throws IOException, TransferFailedException
      {
        // NOOP
      }
    };
    final Blob blob = createStrictMock(Blob.class);
    expect(blob.getCreateTime()).andReturn(time).anyTimes();
    expect(connectionPOJO.storage.get(EasyMock.<BlobId>anyObject()))
        .andReturn(blob).anyTimes();
    replay(blob, connectionPOJO.storage);
    gsWagon.swapAndCloseConnection(connectionPOJO);
    final File file = temporaryFolder.newFile();
    assertFalse(gsWagon.getIfNewer("foo", file, time));
    assertFalse(gsWagon.getIfNewer("foo", file, time + 1));
    assertTrue(gsWagon.getIfNewer("foo", file, time - 1));
    verify(blob, connectionPOJO.storage);
  }

  @Test
  public void testPut() throws Exception
  {
    expect(connectionPOJO.storage.create(EasyMock.anyObject(), EasyMock.<InputStream>anyObject()))
        .andReturn(null)
        .once();
    replay(connectionPOJO.storage);
    gsWagon.swapAndCloseConnection(connectionPOJO);
    gsWagon.put(temporaryFolder.newFile(), "foo");
    verify(connectionPOJO.storage);
  }

  @Test
  public void testBuildStorageFailsWithoutProjectID()
  {
    final GSWagon gsWagon = new GSWagon()
    {
      public Repository getRepository()
      {
        return new Repository("id", "url");
      }
    };
    gsWagon.swapAndCloseConnection(connectionPOJO);
    final String property = gsWagon.getPropertyString(gsWagon.getRepository().getId());
    final String pre = System.getProperty(property);
    try {
      expectedException.expect(new CustomMatcher<Object>(NullPointerException.class.getName())
      {
        @Override
        public boolean matches(Object item)
        {
          return item instanceof NullPointerException;
        }
      });
      System.clearProperty(property);
      gsWagon.swapAndCloseConnection(connectionPOJO);
      assertNotNull(gsWagon.buildStorage(connectionPOJO.client));
    }
    finally {
      if (pre != null) {
        System.setProperty(property, pre);
      } else {
        System.clearProperty(property);
      }
    }
  }

  @Test
  public void testBuildStorage()
  {
    final GSWagon gsWagon = new GSWagon()
    {
      public Repository getRepository()
      {
        return new Repository(repoId, "url");
      }
    };
    gsWagon.swapAndCloseConnection(connectionPOJO);
    final String property = gsWagon.getPropertyString(repoId);
    final String pre = System.getProperty(property);
    try {
      System.setProperty(property, "fakeproject");
      assertNotNull(gsWagon.buildStorage(connectionPOJO.client));
    }
    finally {
      if (pre != null) {
        System.setProperty(property, pre);
      } else {
        System.clearProperty(property);
      }
    }
  }

  @Test
  public void testBuildProperty()
  {
    assertEquals(PROJECT_ID_PROPERTY_PREFIX + repoId, gsWagon.getPropertyString(repoId));
  }

  @Test
  public void testBuildPropertyNPE()
  {
    expectedException.expect(new CustomMatcher<Object>(NullPointerException.class.getName())
    {
      @Override
      public boolean matches(Object item)
      {
        return item instanceof NullPointerException;
      }
    });
    gsWagon.getPropertyString(null);
  }

  @Test
  public void testTranslateNotFound() throws Exception
  {
    expectedException.expect(new CustomMatcher(ResourceDoesNotExistException.class.getName())
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof ResourceDoesNotExistException;
      }
    });
    throw GSWagon.translate("foo", new StorageException(404, "bad"));
  }

  @Test
  public void testTranslateBadAuth401() throws Exception
  {
    expectedException.expect(new CustomMatcher(AuthorizationException.class.getName())
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof AuthorizationException;
      }
    });
    throw GSWagon.translate("foo", new StorageException(401, "bad"));
  }

  @Test
  public void testTranslateBadAuth403() throws Exception
  {
    expectedException.expect(new CustomMatcher(AuthorizationException.class.getName())
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof AuthorizationException;
      }
    });
    throw GSWagon.translate("foo", new StorageException(403, "bad"));
  }

  @Test
  public void testTranslateRetryable() throws Exception
  {
    expectedException.expect(new CustomMatcher(TransferFailedException.class.getName())
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof TransferFailedException;
      }
    });
    final StorageException se = new StorageException(429, "foo");
    assertTrue(se.isRetryable());
    throw GSWagon.translate("foo", se);
  }

  @Test
  public void testTranslateUnknownType() throws Exception
  {
    expectedException.expect(new CustomMatcher(TransferFailedException.class.getName())
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof TransferFailedException;
      }
    });
    final StorageException se = new StorageException(455, "foo");
    assertFalse(se.isRetryable());
    throw GSWagon.translate("foo", se);
  }

  @After
  public void tearDown()
  {
    if (priorId != null) {
      System.setProperty(gsWagon.getPropertyString(repoId), priorId);
    } else {
      System.clearProperty(gsWagon.getPropertyString(repoId));
    }
    priorId = null;
  }

  static ConnectionPOJO createMockConnectionPOJO()
  {
    return new ConnectionPOJO(
        createStrictMock(Storage.class),
        BLOB_ID,
        new MockHttpClient()
    );
  }
}
