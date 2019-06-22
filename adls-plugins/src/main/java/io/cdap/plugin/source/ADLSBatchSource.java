/*
 * Copyright © 2017 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.plugin.common.azurecred.AzureClientSecretService;
import io.cdap.plugin.common.AbstractFileBatchSource;
import io.cdap.plugin.common.FileSourceConfig;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchSource} that reads from Azure Data Lake Store(ADLS).
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("AzureDataLakeStore")
@Description("Batch source to use Azure Data Lake Store(ADLS) as a source.")
public class ADLSBatchSource extends AbstractFileBatchSource {
  private static Logger logger = LoggerFactory.getLogger(ADLSBatchSource.class);

  @SuppressWarnings("unused")
  private final AzureBatchConfig config;

  public ADLSBatchSource(AzureBatchConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Plugin config for {@link ADLSBatchSource}.
   */
  public static class AzureBatchConfig extends FileSourceConfig {
    @Description("Path to file(s) to be read. If a directory is specified,terminate the path name with a '/'. " +
      "The path must start with `adl://`")
    @Macro
    public String path;

    @Nullable
    @Description("The Microsoft Azure Data Lake client id.")
    @Macro
    private String clientId;

    @Nullable
    @Description("The Microsoft Azure Data Lake refresh token URL.")
    @Macro
    private String refreshTokenURL;

    @Nullable
    @Description("The Microsoft Azure Data Lake credentials.")
    @Macro
    private String credentials;

    @Nullable
    @Description("The Microsoft Azure Data Lake credentials.")
    @Macro
    private String keyVaultUrl;

    @Nullable
    @Description("The Microsoft Azure Data Lake credentials.")
    @Macro
    private String kvKeyNames;

    @Override
    protected void validate() {
      super.validate();
      if (!containsMacro("path") && !path.startsWith("adl://")) {
        throw new IllegalArgumentException("Path must start with adl:// for ADLS input files.");
      } 
      /*else if ((!clientId && !refreshTokenURL && !credentials)
                || (!keyVaultUrl && !kvKeyNames)) {
        throw new IllegalArgumentException("Either if client credentials or keyvault details Must be provided.");
      }*/
    }

    @Override
    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = new HashMap<>(super.getFileSystemProperties());
      properties.put("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem");
      properties.put("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");
      properties.put("dfs.adls.oauth2.access.token.provider.type", "ClientCredential");

      if (keyVaultUrl != null && !keyVaultUrl.isEmpty()) {
        Map<String, String> credentials = AzureClientSecretService.getADLSSecretsUsingJceksAndKV(keyVaultUrl, getKvKeyNamesMap(kvKeyNames), properties);
        properties.put("dfs.adls.oauth2.refresh.url", credentials.get("Refresh Token Url Key-Name"));
        properties.put("dfs.adls.oauth2.client.id", credentials.get("Client-Id Key-Name"));
        properties.put("dfs.adls.oauth2.credential", credentials.get("Client Credential Key-Name"));
      } else {
        properties.put("dfs.adls.oauth2.refresh.url", refreshTokenURL);
        properties.put("dfs.adls.oauth2.client.id", clientId);
        properties.put("dfs.adls.oauth2.credential", credentials);
      }
      return properties;
    }

    /* Cretae a map from KeyVault config */
    protected HashMap<String, String> getKvKeyNamesMap(String kvKeyNames) {
      HashMap<String, String> credMap = new HashMap<String, String>();
      String[] keypairs = kvKeyNames.split(",");
      for (String k : keypairs) {
        credMap.put(k.split(":")[1], k.split(":")[0]);
      }
      return credMap;
    }

    @Override
    protected String getPath() {
      return path;
    }
  }
}
