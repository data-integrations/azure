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

package io.cdap.plugin.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.HiveSchemaConverter;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} that writes to Azure Data Lake Store(ADLS).
 */

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("ADLSBatchSink")
@Description("ADLS Batch Sink")
public class ADLSBatchSink extends ReferenceBatchSink<StructuredRecord, Object, Object> {
  @SuppressWarnings("unused")
  private final AzureBatchSinkConfig config;
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String AVRO = "avro";
  private static final String TEXT = "text";
  private static final String ORC = "orc";
  private StructuredToAvroTransformer avroTransformer;
  private StructuredToTextTransformer textTransformer;
  private StructuredToOrcTransformer orcTransformer;

  public ADLSBatchSink(AzureBatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate(pipelineConfigurer.getStageConfigurer().getInputSchema(),
                    pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    Map<String, String> properties = config.getFileSystemProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    conf.set(FileOutputFormat.OUTDIR, config.path);
    job.setOutputValueClass(NullWritable.class);
    if (AVRO.equals(config.outputFormat)) {
      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(config.getSchema().toString());
      AvroJob.setOutputKeySchema(job, avroSchema);
      context.addOutput(Output.of(config.referenceName,
                                  new SinkOutputFormatProvider(AvroKeyOutputFormat.class.getName(), conf)));
    } else if (ORC.equals(config.outputFormat)) {
      StringBuilder builder = new StringBuilder();
      HiveSchemaConverter.appendType(builder, config.getSchema());
      conf.set("orc.mapred.output.schema", builder.toString());
      context.addOutput(Output.of(config.referenceName,
                                  new SinkOutputFormatProvider(OrcOutputFormat.class.getName(), conf)));
    } else {
      context.addOutput(Output.of(config.referenceName,
                                  new SinkOutputFormatProvider(TextOutputFormat.class.getName(), conf)));
    }

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    Schema schema = context.getInputSchema();
    if (schema != null) {
      lineageRecorder.createExternalDataset(schema);

      if (schema.getFields() != null) {
        lineageRecorder.recordWrite("Write", "Write to ADLS",
                                    schema.getFields().stream().map(Schema.Field::getName)
                                      .collect(Collectors.toList()));
      }
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    if (AVRO.equals(config.outputFormat)) {
      avroTransformer = new StructuredToAvroTransformer(config.getSchema());
    } else if (ORC.equals(config.outputFormat)) {
      orcTransformer = new StructuredToOrcTransformer(config.getSchema());
    } else {
      textTransformer = new StructuredToTextTransformer(config.getFieldDelimiter(), config.getSchema());
    }
    super.initialize(context);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Object, Object>> emitter)
    throws Exception {
    if (AVRO.equals(config.outputFormat)) {
      emitter.emit(new KeyValue<>((Object) new AvroKey<>(avroTransformer.transform(input)),
                                  (Object) NullWritable.get()));
    } else if (ORC.equals(config.outputFormat)) {
      emitter.emit(new KeyValue<>((Object) NullWritable.get(), (Object) orcTransformer.transform(input)));
    } else {
      emitter.emit(new KeyValue<>((Object) textTransformer.transform(input), (Object) NullWritable.get()));
    }
  }

  /**
   * Plugin config for {@link ADLSBatchSink}.
   */
  public static class AzureBatchSinkConfig extends ReferencePluginConfig {
    private static final String PATH = "path";
    private static final String SCHEMA = "schema";

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

    @Description("The format of output files.")
    @Macro
    private String outputFormat;

    @Nullable
    @Description("Output schema of the JSON document. Required for avro output format. " +
      "If left empty for text output format, the schema of input records will be used." +
      "This must be a subset of the schema of input records. " +
      "Fields of type ARRAY, MAP, and RECORD are not supported with the text format. " +
      "Fields of type UNION are only supported if they represent a nullable type.")
    @Macro
    public String schema;

    @Nullable
    @Macro
    @Description("Field delimiter for text format output files. Defaults to tab.")
    public String fieldDelimiter;

    public AzureBatchSinkConfig(String referenceName) {
      super(referenceName);
    }

    protected void validate(Schema inputSchema, FailureCollector collector) {
      validate(collector);
      if (!containsMacro(SCHEMA)) {
        for (Schema.Field outputField : getSchema().getFields()) {
          String fieldName = outputField.getName();
          Schema.Field inputField = inputSchema.getField(outputField.getName());
          if (inputField == null) {
            collector.addFailure("Input schema does not contain the '" + fieldName + "' field.", null)
              .withOutputSchemaField(fieldName);
          } else {
            if (TEXT.equals(outputFormat)) {
              Schema inputFieldSchema = inputField.getSchema();
              inputFieldSchema = inputFieldSchema.isNullable() ? inputFieldSchema.getNonNullable() : inputFieldSchema;
              Schema.Type inputType = inputFieldSchema.getType();
              switch (inputType) {
                case ARRAY:
                case MAP:
                case RECORD:
                  collector.addFailure(String.format("Field '%s' is of unexpected type '%s'.", fieldName,
                                                     inputFieldSchema.getDisplayName()),
                                       "Provide type that is not array, map and record.")
                    .withInputSchemaField(fieldName);
              }
            }
          }
        }
      }
    }

    protected void validate(FailureCollector collector) {
      if (!containsMacro(PATH) && !path.startsWith("adl://")) {
        collector.addFailure("Path must start with adl:// for ADLS input files.", null)
          .withConfigProperty(PATH);
      }
      if ((AVRO.equals(outputFormat) || ORC.equals(outputFormat)) && !containsMacro("schema") && schema == null) {
        collector.addFailure("Output schema must be specified for 'avro' or 'orc' format output files.", null)
          .withConfigProperty(SCHEMA);
      }
    }

    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = getProps();
      properties.put("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem");
      properties.put("fs.adl.impl.disable.cache", "true");
      properties.put("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");
      properties.put("dfs.adls.oauth2.access.token.provider.type", "ClientCredential");
      properties.put("dfs.adls.oauth2.refresh.url", refreshTokenURL);
      properties.put("dfs.adls.oauth2.client.id", clientId);
      properties.put("dfs.adls.oauth2.credential", credentials);
      return properties;
    }

    @Nullable
    public Schema getSchema() {
      if (schema == null) {
        return null;
      }
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse output schema.");
      }
    }

    public String getFieldDelimiter() {
      return fieldDelimiter == null ? "\t" : fieldDelimiter;
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
