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
import com.google.api.gax.core.NanoClock;
import com.google.api.gax.core.RetrySettings;
import com.google.auth.http.HttpTransportFactory;
import com.google.cloud.HttpTransportOptions;
import com.google.cloud.ReadChannel;
import com.google.cloud.ServiceOptions;
import com.google.cloud.TransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.HttpClient;
import org.apache.maven.wagon.AbstractWagon;
import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.repository.Repository;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;

public class GSWagon extends AbstractWagon
{
  static final String PROJECT_ID_PROPERTY_PREFIX = "wagon.gs.project.";
  private static final Log LOG = LogFactory.getLog(GSWagon.class);
  private final AtomicReference<ConnectionPOJO> connectionPOJO = new AtomicReference<>(null);

  /**
   * {@inheritDoc}
   */
  protected void openConnectionInternal() throws ConnectionException, AuthenticationException
  {
    final Repository repository = getRepository();
    if (!"gs".equals(repository.getProtocol())) {
      throw new IllegalArgumentException(String.format("Unsupported protocol [%s]", repository.getProtocol()));
    }
    swapAndCloseConnection(new ConnectionPOJO(
        buildStorage(),
        BlobId.of(repository.getHost(), repository.getBasedir()),
        ApacheHttpTransport.newDefaultHttpClient()
    ));
  }

  /**
   * {@inheritDoc}
   */
  protected void closeConnection() throws ConnectionException
  {
    // We don't have great ways to close the Google SDK connection, so just null out the items here
    swapAndCloseConnection(null);
  }

  /**
   * {@inheritDoc}
   */
  public void get(String s, File file)
      throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException
  {
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Fetching [%s] to [%s]", s, file));
    }
    try {
      get(getStorage().get(toBlobID(s)), file);
    }
    catch (StorageException se) {
      throw translate(s, se);
    }
    catch (IOException e) {
      throw new TransferFailedException(String.format("Failed to write to file [%s]", file), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean getIfNewer(String s, File file, long ts)
      throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException
  {
    try {
      final Blob blob = getStorage().get(toBlobID(s));
      if (blob.getCreateTime() <= ts) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("Not fetching, [%s] at [%s] is older than [%s]", s, blob.getCreateTime(), ts));
        }
        return false;
      }
      get(blob, file);
      return true;
    }
    catch (StorageException se) {
      throw translate(s, se);
    }
    catch (IOException ioe) {
      throw new TransferFailedException(String.format("Unable to write [%s]", file), ioe);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void put(File file, String s)
      throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException
  {
    try (InputStream fis = new FileInputStream(file)) {
      final BlobInfo blobInfo = BlobInfo
          .newBuilder(toBlobID(s))
          .build();
      final Blob blob = getStorage().create(blobInfo, fis);
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Created [%s] from [%s]", blob, file));
      }
    }
    catch (StorageException se) {
      throw translate(s, se);
    }
    catch (FileNotFoundException e) {
      throw new ResourceDoesNotExistException(String.format("Not found [%s]", file), e);
    }
    catch (IOException e) {
      throw new TransferFailedException(String.format("Error reading file [%s]", file), e);
    }
  }


  @VisibleForTesting
  void get(Blob blob, File file) throws IOException, TransferFailedException
  {
    final long size = blob.getSize();
    try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      raf.setLength(size);
    }
    try (
        final ReadChannel readChannel = blob.reader();
        final FileChannel fileChannel = FileChannel.open(
            file.toPath(),
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ
        );
    ) {
      final ByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
      long readSize = 0;
      do {
        final int mySize = readChannel.read(byteBuffer);
        if (mySize == -1) {
          throw new TransferFailedException(String.format("Premature EOS after [%s] of [%s] bytes", readSize, size));
        }
        readSize += mySize;
      } while (readSize < size);
    }
  }

  @VisibleForTesting
  static RuntimeException translate(String s, StorageException se)
      throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException
  {
    if (se.isRetryable()) {
      throw new TransferFailedException(String.format("Retryable error for [%s]", s), se);
    }
    if (se.getCode() == 404) {
      throw new ResourceDoesNotExistException(String.format("Not found [%s]", s), se);
    }
    if (se.getCode() == 403 || se.getCode() == 401) {
      throw new AuthorizationException(String.format("Auth failure [%s]", s), se);
    }
    throw new TransferFailedException(String.format("Unknown error [%s]", s), se);
  }

  @VisibleForTesting
  BlobId toBlobID(String resource)
  {
    final BlobId base = getBaseId();
    // getName yields a leading `/`
    return BlobId.of(base.getBucket(), String.format("%s/%s", base.getName(), resource));
  }

  @VisibleForTesting
  RetrySettings buildRetrySettings()
  {
    return ServiceOptions
        .getDefaultRetrySettings()
        .toBuilder()
        // No overrides
        .build();
  }

  @VisibleForTesting
  TransportOptions buildTransportOptions()
  {
    return HttpTransportOptions
        .newBuilder()
        .setReadTimeout(getReadTimeout())
        .setConnectTimeout(getTimeout())
        .setHttpTransportFactory(getTransportFactory())
        .build();
  }

  @VisibleForTesting
  HttpTransportFactory getTransportFactory()
  {
    return () -> new ApacheHttpTransport(getClient());
  }

  @VisibleForTesting
  void swapAndCloseConnection(ConnectionPOJO other)
  {
    final ConnectionPOJO old = connectionPOJO.getAndSet(other);
    if (old != null) {
      old.client.getConnectionManager().shutdown();
    }
  }

  @VisibleForTesting
  Storage getStorage()
  {
    return connectionPOJO.get().storage;
  }

  @VisibleForTesting
  BlobId getBaseId()
  {
    return connectionPOJO.get().baseId;
  }

  @VisibleForTesting
  HttpClient getClient()
  {
    return connectionPOJO.get().client;
  }

  @VisibleForTesting
  Storage buildStorage()
  {
    return StorageOptions
        .newBuilder()
        .setRetrySettings(buildRetrySettings())
        .setTransportOptions(buildTransportOptions())
        .setClock(NanoClock.getDefaultClock())
        .setProjectId(getProjectId())
        .build()
        .getService();
  }

  String getProjectId()
  {
    return getProjectId(getRepository());
  }

  String getProjectId(Repository repository)
  {
    return getProjectId(repository.getId());
  }

  String getProjectId(String repositoryId)
  {
    final String property = getPropertyString(repositoryId);
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Checking project ID property [%s]", property));
    }
    return Preconditions.checkNotNull(System.getProperty(property), property);
  }

  String getPropertyString(String repositoryId)
  {
    return PROJECT_ID_PROPERTY_PREFIX + Preconditions.checkNotNull(repositoryId);
  }
}

class ConnectionPOJO
{
  final Storage storage;
  final BlobId baseId;
  final HttpClient client;

  ConnectionPOJO(Storage storage, BlobId baseId, HttpClient client)
  {
    this.storage = storage;
    this.baseId = baseId;
    this.client = client;
  }
}
