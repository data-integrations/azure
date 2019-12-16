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
 * {@link BatchSource} for Azure Blob Store.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("AzureBlobStore")
@Description("Batch source to read from Azure Blob Storage.")
public class AzureBatchSource extends AbstractFileBatchSource {
  private static final String PATH = "path";

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
    @Description("Path to file(s) to be read. If a directory is specified,terminate the path name with a '/'. " +
      "The path must start with `wasb://` or `wasbs://`.")
    @Macro
    public String path;

    @Description("The Microsoft Azure Storage account to use.")
    @Macro
    private String account;

    @Description("The storage key for the specified container on the specified Azure Storage account. Must be a " +
      "valid base64 encoded storage key provided by Microsoft Azure.")
    @Macro
    private String storageKey;

    @Override
    protected void validate(FailureCollector collector) {
      super.validate(collector);
      if (!containsMacro("path") && (!path.startsWith("wasb://") && !path.startsWith("wasbs://"))) {
        collector.addFailure("Path must start with wasb:// or wasbs:// for Windows Azure Storage Blob input files.", null)
          .withConfigProperty(PATH);
      }
    }

    @Override
    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = new HashMap<>(super.getFileSystemProperties());
      properties.put("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
      properties.put("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
      properties.put(String.format("fs.azure.account.key.%s", account), storageKey);
      return properties;
    }

    @Override
    protected String getPath() {
      return path;
    }
  }
}
