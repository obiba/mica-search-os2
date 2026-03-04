/*
 * Copyright (c) 2024 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.mapping;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A thin fluent wrapper around Jackson's JsonGenerator that mimics the
 * XContentBuilder API used in the mapping classes.
 */
public class MappingBuilder {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final StringWriter writer;
  private final JsonGenerator generator;

  private MappingBuilder(StringWriter writer, JsonGenerator generator) {
    this.writer = writer;
    this.generator = generator;
  }

  public static MappingBuilder jsonBuilder() throws IOException {
    StringWriter sw = new StringWriter();
    JsonGenerator gen = MAPPER.getFactory().createGenerator(sw);
    return new MappingBuilder(sw, gen);
  }

  public MappingBuilder startObject() throws IOException {
    generator.writeStartObject();
    return this;
  }

  public MappingBuilder startObject(String name) throws IOException {
    generator.writeFieldName(name);
    generator.writeStartObject();
    return this;
  }

  public MappingBuilder endObject() throws IOException {
    generator.writeEndObject();
    return this;
  }

  public MappingBuilder startArray(String name) throws IOException {
    generator.writeFieldName(name);
    generator.writeStartArray();
    return this;
  }

  public MappingBuilder endArray() throws IOException {
    generator.writeEndArray();
    return this;
  }

  public MappingBuilder field(String name, String value) throws IOException {
    generator.writeStringField(name, value);
    return this;
  }

  public MappingBuilder field(String name) throws IOException {
    generator.writeFieldName(name);
    return this;
  }

  @Override
  public String toString() {
    try {
      generator.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return writer.toString();
  }
}
