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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import io.cdap.plugin.format.plugin.AbstractFileSource;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link BatchSource} for Azure Blob Store and Azure Data Lake Storage Gen2.
 *
 * Supported path schemes:
 *   wasb:// / wasbs://  — Azure Blob Storage (WASB driver)
 *   abfs:// / abfss://  — Azure Data Lake Storage Gen2 (ABFS driver)
 *
 * Supported authentication methods:
 *   storageAccountKey  — shared key; works with wasb and abfs paths
 *   sasToken           — SAS token; wasb paths only
 *   servicePrincipal   — Azure AD OAuth2 client credentials; abfs paths only
 *   managedIdentity    — Azure AD managed identity; abfs paths only
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("AzureBlobStore")
@Description("Batch source to read from Azure Blob Storage or Azure Data Lake Storage Gen2.")
public class AzureBatchSource extends AbstractFileSource<AzureBatchSource.AzureBatchConfig> {

  private final AzureBatchConfig config;
  private Asset asset;

  public AzureBatchSource(AzureBatchConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    asset = Asset.builder(config.getReferenceName())
      .setFqn(config.getPath()).build();
    super.prepareRun(context);
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    return new LineageRecorder(context, asset);
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    Map<String, String> properties = new HashMap<>(config.getFilesystemProperties());

    String path = config.getPath();

    if (path.startsWith("wasb://") || path.startsWith("wasbs://")) {
      properties.put("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
      properties.put("fs.wasb.impl.disable.cache", "true");
      properties.put("fs.wasbs.impl.disable.cache", "true");
      properties.put("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
    } else if (path.startsWith("abfs://") || path.startsWith("abfss://")) {
      properties.put("fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
      properties.put("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
      properties.put("fs.AbstractFileSystem.abfs.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
      properties.put("fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfss");
    }

    String authMethod = config.authenticationMethod;
    String account = config.account;

    if (AzureBatchConfig.AUTH_STORAGE_ACCOUNT_KEY.equalsIgnoreCase(authMethod)) {
      properties.put(String.format("fs.azure.account.key.%s", account), config.storageKey);

    } else if (AzureBatchConfig.AUTH_SAS_TOKEN.equalsIgnoreCase(authMethod)) {
      properties.put(String.format("fs.azure.sas.%s.%s", config.container, account), config.sasToken);

    } else if (AzureBatchConfig.AUTH_SERVICE_PRINCIPAL.equalsIgnoreCase(authMethod)) {
      properties.put(String.format("fs.azure.account.auth.type.%s", account), "OAuth");
      properties.put(String.format("fs.azure.account.oauth.provider.type.%s", account),
                     "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
      properties.put(String.format("fs.azure.account.oauth2.client.endpoint.%s", account),
                     String.format("https://login.microsoftonline.com/%s/oauth2/token", config.tenantId));
      properties.put(String.format("fs.azure.account.oauth2.client.id.%s", account), config.clientId);
      properties.put(String.format("fs.azure.account.oauth2.client.secret.%s", account), config.clientSecret);

    } else if (AzureBatchConfig.AUTH_MANAGED_IDENTITY.equalsIgnoreCase(authMethod)) {
      properties.put(String.format("fs.azure.account.auth.type.%s", account), "OAuth");
      properties.put(String.format("fs.azure.account.oauth.provider.type.%s", account),
                     "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider");
    }

    if (config.shouldCopyHeader()) {
      properties.put(PathTrackingInputFormat.COPY_HEADER, "true");
    }
    if (config.getFileEncoding() != null && !config.getFileEncoding().equals(config.getDefaultFileEncoding())) {
      properties.put(PathTrackingInputFormat.SOURCE_FILE_ENCODING, config.getFileEncoding());
    }
    return properties;
  }

  @Override
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordRead("Read", "Read from Azure Blob Storage.", outputFields);
  }

  @Override
  protected boolean shouldGetSchema() {
    return !config.containsMacro(AzureBatchConfig.NAME_PATH)
      && !config.containsMacro("format")
      && !config.containsMacro("delimiter")
      && !config.containsMacro(AzureBatchConfig.NAME_ACCOUNT)
      && !config.containsMacro(AzureBatchConfig.NAME_STORAGE_KEY)
      && !config.containsMacro(AzureBatchConfig.NAME_SAS_TOKEN)
      && !config.containsMacro(AzureBatchConfig.NAME_TENANT_ID)
      && !config.containsMacro(AzureBatchConfig.NAME_CLIENT_ID)
      && !config.containsMacro(AzureBatchConfig.NAME_CLIENT_SECRET)
      && !config.containsMacro(AzureBatchConfig.NAME_FILE_SYSTEM_PROPERTIES);
  }

  /**
   * Plugin config for {@link AzureBatchSource}.
   */
  public static class AzureBatchConfig extends AbstractFileSourceConfig {

    public static final String NAME_PATH = "path";
    public static final String NAME_ACCOUNT = "account";
    public static final String NAME_STORAGE_KEY = "storageKey";
    public static final String NAME_SAS_TOKEN = "sasToken";
    public static final String NAME_TENANT_ID = "tenantId";
    public static final String NAME_CLIENT_ID = "clientId";
    public static final String NAME_CLIENT_SECRET = "clientSecret";
    public static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";

    static final String AUTH_STORAGE_ACCOUNT_KEY = "storageAccountKey";
    static final String AUTH_SAS_TOKEN = "sasToken";
    static final String AUTH_SERVICE_PRINCIPAL = "servicePrincipal";
    static final String AUTH_MANAGED_IDENTITY = "managedIdentity";

    private static final String NAME_AUTHENTICATION_METHOD = "authenticationMethod";
    private static final String NAME_CONTAINER = "container";
    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    @Description("Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'. " +
      "Supports wasb://, wasbs://, abfs://, and abfss:// schemes. " +
      "Example WASB: wasb://mycontainer@myaccount.blob.core.windows.net/path/. " +
      "Example ABFS: abfss://mycontainer@myaccount.dfs.core.windows.net/path/.")
    @Macro
    private String path;

    @Description("The Microsoft Azure Storage account to use. " +
      "For WASB paths, must end with `.blob.core.windows.net` " +
      "(e.g. `mystorageaccount.blob.core.windows.net`). " +
      "For ABFS paths, must end with `.dfs.core.windows.net` " +
      "(e.g. `mystorageaccount.dfs.core.windows.net`).")
    @Macro
    private String account;

    @Description("The authentication method to use to connect to Microsoft Azure. " +
      "'Storage Account Key' and 'SAS Token' work with wasb:// paths. " +
      "'Service Principal' and 'Managed Identity' work with abfs:// paths. " +
      "Defaults to 'Storage Account Key'.")
    private String authenticationMethod;

    // --- Storage Account Key ---

    @Description("The storage key for the container on the Microsoft Azure Storage account. " +
      "Must be a valid base64 encoded storage key provided by Microsoft Azure. " +
      "Required when authentication method is 'Storage Account Key'.")
    @Nullable
    @Macro
    private String storageKey;

    // --- SAS Token ---

    @Description("The SAS token to use to connect to the specified container. " +
      "Required when authentication method is 'SAS Token'.")
    @Nullable
    @Macro
    private String sasToken;

    @Description("The container to connect to. Required when authentication method is 'SAS Token'.")
    @Nullable
    @Macro
    private String container;

    // --- Azure AD: Service Principal ---

    @Description("The Azure Active Directory tenant (directory) ID. " +
      "Required when authentication method is 'Service Principal'.")
    @Nullable
    @Macro
    private String tenantId;

    @Description("The client (application) ID of the Azure AD service principal. " +
      "Required when authentication method is 'Service Principal'.")
    @Nullable
    @Macro
    private String clientId;

    @Description("The client secret of the Azure AD service principal. " +
      "Required when authentication method is 'Service Principal'.")
    @Nullable
    @Macro
    private String clientSecret;

    // --- Extra FS properties ---

    @Description("Any additional properties to use when reading from the filesystem. " +
      "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
    @Nullable
    @Macro
    private String fileSystemProperties;

    public AzureBatchConfig() {
      fileSystemProperties = GSON.toJson(Collections.emptyMap());
    }

    @Override
    public void validate(FailureCollector collector) {
      super.validate(collector);

      boolean isWasb = !containsMacro(NAME_PATH) &&
        (path.startsWith("wasb://") || path.startsWith("wasbs://"));
      boolean isAbfs = !containsMacro(NAME_PATH) &&
        (path.startsWith("abfs://") || path.startsWith("abfss://"));

      if (!containsMacro(NAME_PATH) && !isWasb && !isAbfs) {
        collector.addFailure(
          "Path must start with wasb://, wasbs://, abfs://, or abfss://.", null)
          .withConfigProperty(NAME_PATH);
      }

      if (!containsMacro(NAME_ACCOUNT)) {
        if (isWasb && !account.endsWith(".blob.core.windows.net")) {
          collector.addFailure(
            "Account must end with '.blob.core.windows.net' for wasb:// paths.", null)
            .withConfigProperty(NAME_ACCOUNT);
        }
        if (isAbfs && !account.endsWith(".dfs.core.windows.net")) {
          collector.addFailure(
            "Account must end with '.dfs.core.windows.net' for abfs:// paths.", null)
            .withConfigProperty(NAME_ACCOUNT);
        }
      }

      boolean validAuthMethod = AUTH_STORAGE_ACCOUNT_KEY.equalsIgnoreCase(authenticationMethod)
        || AUTH_SAS_TOKEN.equalsIgnoreCase(authenticationMethod)
        || AUTH_SERVICE_PRINCIPAL.equalsIgnoreCase(authenticationMethod)
        || AUTH_MANAGED_IDENTITY.equalsIgnoreCase(authenticationMethod);

      if (!validAuthMethod) {
        collector.addFailure(
          "Authentication method must be one of 'Storage Account Key', 'SAS Token', " +
            "'Service Principal', or 'Managed Identity'.", null)
          .withConfigProperty(NAME_AUTHENTICATION_METHOD);
      }

      if (AUTH_STORAGE_ACCOUNT_KEY.equalsIgnoreCase(authenticationMethod)) {
        if (!containsMacro(NAME_STORAGE_KEY) && Strings.isNullOrEmpty(storageKey)) {
          collector.addFailure(
            "Storage key must be provided when authentication method is 'Storage Account Key'.", null)
            .withConfigProperty(NAME_STORAGE_KEY);
        }
      }

      if (AUTH_SAS_TOKEN.equalsIgnoreCase(authenticationMethod)) {
        if (!containsMacro(NAME_PATH) && isAbfs) {
          collector.addFailure("SAS Token authentication is only supported for wasb:// and wasbs:// paths.", null)
            .withConfigProperty(NAME_AUTHENTICATION_METHOD);
        }
        if (!containsMacro(NAME_SAS_TOKEN) && Strings.isNullOrEmpty(sasToken)) {
          collector.addFailure(
            "SAS token must be provided when authentication method is 'SAS Token'.", null)
            .withConfigProperty(NAME_SAS_TOKEN);
        }
        if (!containsMacro(NAME_CONTAINER) && Strings.isNullOrEmpty(container)) {
          collector.addFailure(
            "Container must be provided when authentication method is 'SAS Token'.", null)
            .withConfigProperty(NAME_CONTAINER);
        }
      }

      if (AUTH_SERVICE_PRINCIPAL.equalsIgnoreCase(authenticationMethod)
        || AUTH_MANAGED_IDENTITY.equalsIgnoreCase(authenticationMethod)) {
        if (!containsMacro(NAME_PATH) && isWasb) {
          collector.addFailure(
            "Azure AD authentication (Service Principal / Managed Identity) requires abfs:// or abfss:// paths.", null)
            .withConfigProperty(NAME_AUTHENTICATION_METHOD);
        }
      }

      if (AUTH_SERVICE_PRINCIPAL.equalsIgnoreCase(authenticationMethod)) {
        if (!containsMacro(NAME_TENANT_ID) && Strings.isNullOrEmpty(tenantId)) {
          collector.addFailure(
            "Tenant ID must be provided when authentication method is 'Service Principal'.", null)
            .withConfigProperty(NAME_TENANT_ID);
        }
        if (!containsMacro(NAME_CLIENT_ID) && Strings.isNullOrEmpty(clientId)) {
          collector.addFailure(
            "Client ID must be provided when authentication method is 'Service Principal'.", null)
            .withConfigProperty(NAME_CLIENT_ID);
        }
        if (!containsMacro(NAME_CLIENT_SECRET) && Strings.isNullOrEmpty(clientSecret)) {
          collector.addFailure(
            "Client secret must be provided when authentication method is 'Service Principal'.", null)
            .withConfigProperty(NAME_CLIENT_SECRET);
        }
      }

      if (!containsMacro(NAME_FILE_SYSTEM_PROPERTIES)) {
        try {
          getFilesystemProperties();
        } catch (Exception e) {
          collector.addFailure("File system properties must be a valid JSON.", null)
            .withConfigProperty(NAME_FILE_SYSTEM_PROPERTIES).withStacktrace(e.getStackTrace());
        }
      }
    }

    @Override
    public String getPath() {
      return path;
    }

    Map<String, String> getFilesystemProperties() {
      if (containsMacro(NAME_FILE_SYSTEM_PROPERTIES) || Strings.isNullOrEmpty(fileSystemProperties)) {
        return new HashMap<>();
      }
      return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
  }
}
