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

package co.cask.hydrator.plugin.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * ADLS batch sink
 */

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("ADLSBatchSink")
@Description("ADLS Batch Sink")
public class ADLSBatchSink extends ReferenceBatchSink<StructuredRecord, AvroKey<GenericRecord>, NullWritable> {
  @SuppressWarnings("unused")
  private final AzureBatchSinkConfig config;
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private StructuredToAvroTransformer avroTransformer;

  public ADLSBatchSink(AzureBatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    config.validate();
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    Map<String, String> properties = config.getFileSystemProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    conf.set(FileOutputFormat.OUTDIR, config.path);
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(config.getSchema().toString());
    AvroJob.setOutputKeySchema(job, avroSchema);
    job.setOutputValueClass(NullWritable.class);

    context.addOutput(Output.of(config.referenceName,
                                new SinkOutputFormatProvider(AvroKeyOutputFormat.class.getName(), conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    avroTransformer = new StructuredToAvroTransformer(config.getSchema().toString());
    super.initialize(context);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter)
    throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(avroTransformer.transform(input)), NullWritable.get()));
  }

  /**
   * Plugin config for {@link ADLSBatchSink}.
   */
  public static class AzureBatchSinkConfig extends ReferencePluginConfig {
    protected static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "A JSON string representing a map of properties " +
      "needed for the distributed file system.";

    @Description("Path to directory to store output files. The path must start with `adl://`")
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

    @Nullable
    @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
    @Macro
    public String fileSystemProperties;

    @Description("Output schema for the output files.")
    @Macro
    public String schema;

    public AzureBatchSinkConfig(String referenceName) {
      super(referenceName);
    }

    protected void validate() {
      if (!containsMacro("path") && !path.startsWith("adl://")) {
        throw new IllegalArgumentException("Path must start with adl:// for ADLS input files.");
      }
    }

    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = getProps();
      properties.put("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem");
      properties.put("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");
      properties.put("dfs.adls.oauth2.access.token.provider.type", "ClientCredential");
      properties.put("dfs.adls.oauth2.refresh.url", refreshTokenURL);
      properties.put("dfs.adls.oauth2.client.id", clientId);
      properties.put("dfs.adls.oauth2.credential", credentials);
      return properties;
    }

    public Schema getSchema() {
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse output schema.");
      }
    }

    protected Map<String, String> getProps() {
      if (fileSystemProperties == null) {
        return new HashMap<>();
      }
      try {
        return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse fileSystemProperties: " + e.getMessage());
      }
    }
  }
}
