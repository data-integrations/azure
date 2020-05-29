/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.source;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.plugin.common.AbstractFileBatchSource;
import io.cdap.plugin.common.FileSourceConfig;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link BatchSource} for Azure Blob Store.
 */
@Plugin(type = "batchsource")
@Name("AzureBlobStore")
@Description("Batch source to read from Azure Blob Storage.")
public class AzureBatchSource extends AbstractFileBatchSource {

  @SuppressWarnings("unused")
  private final AzureBatchConfig config;

  public AzureBatchSource(AzureBatchConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Plugin config for {@link AzureBatchSource}.
   */
  public static class AzureBatchConfig extends FileSourceConfig {
    private static final String PATH = "path";
    private static final String AUTHENTICATION_METHOD = "authenticationMethod";
    private static final String STORAGE_ACCOUNT_KEY = "storageKey";
    private static final String SAS_TOKEN = "sasToken";
    private static final String STORAGE_ACCOUNT_KEY_AUTH_METHOD = "storageAccountKey";
    private static final String SAS_TOKEN_AUTH_METHOD = "sasToken";

    @Description("Path to file(s) to be read. If a directory is specified,terminate the path name with a '/'. " +
      "The path must start with `wasb://` or `wasbs://`.")
    @Macro
    public String path;

    @Description("The Microsoft Azure Storage account to use.")
    @Macro
    private String account;

    @Description("The authentication method to use to connect to Microsoft Azure. Can be either storageAccountKey " +
      "or sasToken. Defaults to storageAccountKey.")
    @Nullable
    @Macro
    private String authenticationMethod;

    @Description("The storage key for the specified container on the specified Azure Storage account. Must be a " +
      "valid base64 encoded storage key provided by Microsoft Azure.")
    @Macro
    private String storageKey;

    @Description("The SAS token to use to connect to the specified container. Required when authentication method " +
      "is set to SAS Token.")
    @Nullable
    @Macro
    private String sasToken;

    @Description("The container to connect to. Required when authentication method is set to SAS Token.")
    @Nullable
    @Macro
    private String container;

    @Override
    protected void validate(FailureCollector collector) {
      super.validate(collector);
      if (!containsMacro("path") && (!path.startsWith("wasb://") && !path.startsWith("wasbs://"))) {
        collector.addFailure("Path must start with wasb:// or wasbs:// for ADLS input files.", null)
          .withConfigProperty(PATH);
      }
      if (!containsMacro("authType")) {
        if (!(STORAGE_ACCOUNT_KEY_AUTH_METHOD.equalsIgnoreCase(authenticationMethod) ||
          SAS_TOKEN_AUTH_METHOD.equalsIgnoreCase(authenticationMethod))) {
          collector.addFailure(
            String.format(
              "Authentication method should be one of '%s' or '%s'",
              STORAGE_ACCOUNT_KEY_AUTH_METHOD,
              SAS_TOKEN_AUTH_METHOD
            ), null)
            .withConfigProperty(AUTHENTICATION_METHOD);
        }
        if (STORAGE_ACCOUNT_KEY_AUTH_METHOD.equalsIgnoreCase(authenticationMethod) &&
          !containsMacro(storageKey) && Strings.isNullOrEmpty(storageKey)) {
          collector.addFailure(
            String.format("Storage key must be provided when authentication method is set to %s",
                          STORAGE_ACCOUNT_KEY_AUTH_METHOD),
            null
          ).withConfigProperty(STORAGE_ACCOUNT_KEY);
        }
        if (SAS_TOKEN_AUTH_METHOD.equalsIgnoreCase(authenticationMethod) &&
          !containsMacro(sasToken) && Strings.isNullOrEmpty(sasToken)) {
          collector.addFailure(
            String.format("Storage key must be provided when authentication method is set to %s",
                          SAS_TOKEN_AUTH_METHOD),
            null
          ).withConfigProperty(SAS_TOKEN);
        }
      }
    }

    @Override
    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = new HashMap<>(super.getFileSystemProperties());
      properties.put("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
      properties.put("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
      if (STORAGE_ACCOUNT_KEY_AUTH_METHOD.equalsIgnoreCase(authenticationMethod)) {
        properties.put(String.format("fs.azure.account.key.%s", account), storageKey);
      } else if (SAS_TOKEN_AUTH_METHOD.equalsIgnoreCase(authenticationMethod)) {
        properties.put(String.format("fs.azure.sas.%s.%s", container, account), sasToken);
      }
      return properties;
    }

    @Override
    protected String getPath() {
      return path;
    }
  }
}
