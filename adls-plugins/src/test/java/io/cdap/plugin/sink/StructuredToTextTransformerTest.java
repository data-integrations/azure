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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test Structured Record to Text converter
 */
public class StructuredToTextTransformerTest {
  private static final Schema FULL_SCHEMA =
    Schema.recordOf("full",
                    Schema.Field.of("bool_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("enum_field", Schema.nullableOf(Schema.enumWith("A", "B", "C"))),
                    Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.INT))),
                    Schema.Field.of("union", Schema.unionOf(Schema.of(Schema.Type.NULL), Schema.of(Schema.Type.STRING)))
    );

  private static final Schema VALID_SCHEMA =
    Schema.recordOf("valid",
                    Schema.Field.of("bool_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("enum_field", Schema.nullableOf(Schema.enumWith("A", "B", "C")))
    );

  private static final StructuredRecord NONNULL_RECORD = StructuredRecord.builder(FULL_SCHEMA)
    .set("bool_field", false)
    .set("int_field", 1)
    .set("long_field", 1L)
    .set("float_field", 1.0)
    .set("double_field", 1.0)
    .set("bytes_field", new byte[]{})
    .set("string_field", "String")
    .set("enum_field", "A")
    .set("array", new int[]{1, 2, 3})
    .set("map", ImmutableMap.of("1", 1, "2", 2, "3", 3))
    .set("union", null).build();
  private static final String NONNULL_RECORD_STRING = "false\t1\t1\t1.0\t1.0\t\tString\tA";
  private static final StructuredRecord NULL_RECORD = StructuredRecord.builder(FULL_SCHEMA)
    .set("bool_field", null)
    .set("int_field", null)
    .set("long_field", 1L)
    .set("float_field", 1.0)
    .set("double_field", 1.0)
    .set("bytes_field", null)
    .set("string_field", "String")
    .set("enum_field", "A")
    .set("array", new int[]{1, 2, 3})
    .set("map", ImmutableMap.of("1", 1, "2", 2, "3", 3))
    .set("union", null).build();
  private static final String NULL_RECORD_STRING = "\t\t1\t1.0\t1.0\t\tString\tA";

  private static final Schema INVALID_SCHEMA =
    Schema.recordOf("invalid", Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.INT))));

  @Test
  public void testInvalidOutputSchema() throws Exception {
    // Without specifying the output schema, NONNULL_RECORD's schema FULL_SCHEMA will be used as output schema,
    // which contains invalid types
    assertInvalidOutputSchema(new StructuredToTextTransformer("\t", null), NONNULL_RECORD);
    // Use INVALID_SCHEMA as output schema, which contains invalid type ARRAY
    assertInvalidOutputSchema(new StructuredToTextTransformer("\t", INVALID_SCHEMA), NONNULL_RECORD);
  }

  @Test
  public void testTextTransform() throws Exception {
    StructuredToTextTransformer transformer = new StructuredToTextTransformer("\t", VALID_SCHEMA);
    Assert.assertEquals(NONNULL_RECORD_STRING, transformer.transform(NONNULL_RECORD).toString());
    Assert.assertEquals(NULL_RECORD_STRING, transformer.transform(NULL_RECORD).toString());
  }

  private static void assertInvalidOutputSchema(StructuredToTextTransformer transformer, StructuredRecord record)
    throws IOException {
    try {
      transformer.transform(record);
      Assert.fail();
    } catch (UnexpectedFormatException e) {
      // expected
    }
  }
}
