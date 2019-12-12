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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.plugin.common.AbstractFileBatchSource;
import io.cdap.plugin.common.FileSourceConfig;


import java.util.HashMap;
import java.util.Map;

/**
 * A {@link BatchSource} that reads from Azure Data Lake Store(ADLS).
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("AzureDataLakeStore")
@Description("Batch source to use Azure Data Lake Store(ADLS) as a source.")
public class ADLSBatchSource extends AbstractFileBatchSource {
  private static final String PATH = "path";

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

    @Description("The Microsoft Azure Data Lake client id.")
    @Macro
    private String clientId;

    @Description("The Microsoft Azure Data Lake refresh token URL.")
    @Macro
    private String refreshTokenURL;

    @Description("The Microsoft Azure Data Lake credentials.")
    @Macro
    private String credentials;

    @Override
    protected void validate(FailureCollector collector) {
      super.validate(collector);
      if (!containsMacro("path") && !path.startsWith("adl://")) {
        collector.addFailure("Path must start with adl:// for ADLS input files.", null).withConfigProperty(PATH);
      }
    }

    @Override
    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = new HashMap<>(super.getFileSystemProperties());
      properties.put("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem");
      properties.put("fs.adl.impl.disable.cache", "true");
      properties.put("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");
      properties.put("dfs.adls.oauth2.access.token.provider.type", "ClientCredential");
      properties.put("dfs.adls.oauth2.refresh.url", refreshTokenURL);
      properties.put("dfs.adls.oauth2.client.id", clientId);
      properties.put("dfs.adls.oauth2.credential", credentials);
      return properties;
    }

    @Override
    protected String getPath() {
      return path;
    }
  }
}
