/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import org.apache.beam.examples.common.Employee;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;

public class XMLInfoExtractor {

  static class TransformEmployeeToString extends DoFn<Employee, String> {

    @ProcessElement
    public void processElement(@Element Employee employee, OutputReceiver<String> receiver) {
      receiver.output(employee.getName() + "-" + employee.getId());
    }
  }


  public static class EmployeeToString
          extends PTransform<PCollection<Employee>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<Employee> employees) {

      // Convert lines of text into individual words.
      PCollection<String> employeesAsString = employees.apply(ParDo.of(new TransformEmployeeToString()));

      return employeesAsString;
    }
  }

    public interface XMLInfoExtractorOptions extends PipelineOptions {

      /**
       * By default, this example reads from a public dataset containing the text of King Lear. Set
       * this option to choose a different input file or glob.
       */
      @Description("Path of the file to read from")
      @Default.String("employees.xml")
      String getInputFile();

      void setInputFile(String value);

      /**
       * Set this required option to specify where to write the output.
       */
      @Description("Path of the file to write to")
      @Required
      String getOutput();

      void setOutput(String value);
    }

  private static Mutation makeMutation(String name, String id) {
    String rowId = name+id;
    byte[] rowKey = rowId.getBytes(StandardCharsets.UTF_8);
    return new Put(rowKey)
            .addColumn(COLUMN_FAMILY, COLUMN_NAME, Bytes.toBytes(name))
            .addColumn(COLUMN_FAMILY, COLUMN_ID, Bytes.toBytes(id));
  }

  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("personaldata");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("name");
  private static final byte[] COLUMN_ID = Bytes.toBytes("id");
  private static final String TABLE_NAME = "employees";

    static void runXmlInfoExtractor(XMLInfoExtractorOptions options) {
      Pipeline p = Pipeline.create(options);

      final Configuration conf = HBaseConfiguration.create();


      p.apply("ReadXML", XmlIO.<Employee>read().from(options.getInputFile())
              .withRootElement("report")
              .withRecordElement("employee")
              .withRecordClass(Employee.class))
              .apply("ToHBaseMutation", MapElements.via(new SimpleFunction<Employee, Mutation>() {
                @Override
                public Mutation apply(Employee employee) {
                  return makeMutation(employee.getName(), employee.getId());
                }
              }))
              /*.apply(new EmployeeToString())
              .apply("WriteEmployees", TextIO.write().to(options.getOutput()));*/
              .apply(HBaseIO.write().withConfiguration(conf).withTableId(TABLE_NAME));

      p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
      XMLInfoExtractorOptions options =
              PipelineOptionsFactory.fromArgs(args).withValidation().as(XMLInfoExtractorOptions.class);

      runXmlInfoExtractor(options);
    }

}
