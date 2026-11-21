/*
 * Copyright © 2026 Cask Data, Inc.
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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.Writer;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcMapreduceRecordReader;
import org.apache.orc.mapreduce.OrcMapreduceRecordWriter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Writes records produced by {@link StructuredToOrcTransformer} to an ORC file and reads them back.
 */
public class StructuredToOrcTransformerTest {

  private static final Schema SCHEMA =
    Schema.recordOf("record",
                    Schema.Field.of("bool_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                    Schema.Field.of("int_field", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("long_field", Schema.of(Schema.Type.LONG)),
                    Schema.Field.of("double_field", Schema.of(Schema.Type.DOUBLE)),
                    Schema.Field.of("bytes_field", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)));

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testWriteAndReadOrcFile() throws Exception {
    StructuredToOrcTransformer transformer = new StructuredToOrcTransformer(SCHEMA);
    Configuration conf = new Configuration();
    Path path = new Path(new File(tmpFolder.getRoot(), "test.orc").toURI());

    OrcStruct first = transformer.transform(record(true, 1, "a"));
    Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(first.getSchema()));
    OrcMapreduceRecordWriter<OrcStruct> recordWriter = new OrcMapreduceRecordWriter<>(writer);
    recordWriter.write(NullWritable.get(), first);
    recordWriter.write(NullWritable.get(), transformer.transform(record(null, 2, "b")));
    recordWriter.close(null);

    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    Assert.assertEquals(2, reader.getNumberOfRows());
    Assert.assertEquals("struct<bool_field:boolean,int_field:int,long_field:bigint,double_field:double,"
                          + "bytes_field:binary,string_field:string>", reader.getSchema().toString());

    OrcMapreduceRecordReader<OrcStruct> recordReader = new OrcMapreduceRecordReader<>(reader, reader.options());
    Assert.assertTrue(recordReader.nextKeyValue());
    OrcStruct row = recordReader.getCurrentValue();
    Assert.assertEquals("true", row.getFieldValue("bool_field").toString());
    Assert.assertEquals("1", row.getFieldValue("int_field").toString());
    Assert.assertEquals("a", row.getFieldValue("string_field").toString());
    Assert.assertTrue(recordReader.nextKeyValue());
    row = recordReader.getCurrentValue();
    Assert.assertNull(row.getFieldValue("bool_field"));
    Assert.assertEquals("2", row.getFieldValue("int_field").toString());
    Assert.assertEquals("b", row.getFieldValue("string_field").toString());
    Assert.assertFalse(recordReader.nextKeyValue());
    recordReader.close();
  }

  private static StructuredRecord record(Boolean bool, int i, String s) {
    return StructuredRecord.builder(SCHEMA)
      .set("bool_field", bool)
      .set("int_field", i)
      .set("long_field", (long) i)
      .set("double_field", (double) i)
      .set("bytes_field", s.getBytes())
      .set("string_field", s)
      .build();
  }
}
