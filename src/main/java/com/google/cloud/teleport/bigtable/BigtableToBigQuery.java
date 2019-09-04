/*
 * Copyright (C) 2019 Google Inc.
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

package com.google.cloud.teleport.bigtable;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.TimestampRange;
import com.google.cloud.teleport.util.BytesUtils;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Dataflow pipeline that exports data from a Cloud Bigtable table to a BigQuery table. Currently,
 * filtering on Cloud Bigtable table is not supported.
 *
 * <p>Example pipeline run configurations:
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * GCS_PATH=gs://${PROJECT_ID}/dataflow/pipelines/bt2bq
 * BIGTABLE_INSTANCE_ID=bigtable-instance-id
 * BIGTABLE_TABLE_ID=bigtable-table-name
 * BIGTABLE_START_TIME_FILTER=2017-06-18T00:00:00+01:00
 * BIGTABLE_END_TIME_FILTER=2017-06-19T00:00:00+01:00
 * BIGTABLE_COLUMN_FAMILY_FILTER=cf
 * BIGTABLE_ROW_FILTER=rowKey
 *
 * BIGQUERY_TABLE=dataset_name.table_name
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 *  -Dexec.mainClass=com.google.cloud.teleport.bigtable.BigtableToBigQuery \
 *  -Dexec.cleanupDaemonThreads=false \
 *  -Dexec.args=" \
 *  --region=europe-west1 \
 *  --project=${PROJECT_ID} \
 *  --stagingLocation=gs://${GCS_PATH}/staging/ \
 *  --tempLocation=gs://${GCS_PATH}/tmp/ \
 *  --runner=${RUNNER} \
 *  --bigtableProjectId=${PROJECT_ID} \
 *  --bigtableInstanceId=${BIGTABLE_INSTANCE_ID} \
 *  --bigtableTableId=${BIGTABLE_TABLE_ID} \
 *  --bigtableStartTime=${BIGTABLE_START_TIME_FILTER} \
 *  --bigtableEndTime=${BIGTABLE_END_TIME_FILTER} \
 *  --familyNameRegexFilter=${BIGTABLE_COLUMN_FAMILY_FILTER} \
 *  --columnQualifierRegexFilter=${BIGTABLE_ROW_FILTER} \
 *  --outputTableSpec=${BIGQUERY_TABLE} \
 *  --bigQueryLoadingTemporaryDirectory=gs://${GCS_PATH}/load_tmp/"
 * </pre>
 */
public class BigtableToBigQuery {

  /** Target BigQuery table schema. */
  private static final TableSchema SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("load_time").setType("TIMESTAMP"),
                  new TableFieldSchema().setName("key").setType("BYTES"),
                  new TableFieldSchema()
                      .setName("cells")
                      .setType("RECORD")
                      .setMode("REPEATED")
                      .setFields(
                          ImmutableList.of(
                              new TableFieldSchema().setName("family").setType("STRING"),
                              new TableFieldSchema().setName("qualifier").setType("BYTES"),
                              new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"),
                              new TableFieldSchema().setName("value").setType("BYTES")))));

  private static final DateTimeFormatter DATETIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
          .toFormatter()
          .withZone(ZoneOffset.UTC);

  /** Options for the export pipeline. */
  public interface Options extends PipelineOptions {
    @Validation.Required
    @Description("The project that contains the table to export.")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @Validation.Required
    @Description("The Bigtable instance id that contains the table to export.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @Validation.Required
    @Description("The Bigtable table id to export.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @Description(
        "Filter BigTable cells from this timestamp inclusive. "
            + "If left empty, interpreted as 0. "
            + "Expects a string in ISO-8601 extended offset format, e.g. '2018-12-03T10:15:30+01:00'.")
    ValueProvider<String> getBigtableStartTime();

    @SuppressWarnings("unused")
    void setBigtableStartTime(ValueProvider<String> startTime);

    @Description(
        "Filter BigTable cells until this timestamp exclusive. "
            + "If left empty, interpreted as infinity. "
            + "Expects a string in ISO-8601 extended offset format, e.g. '2018-12-03T10:15:30+01:00'.")
    ValueProvider<String> getBigtableEndTime();

    @SuppressWarnings("unused")
    void setBigtableEndTime(ValueProvider<String> endTime);

    @Description("Filter cells by BigTable column family based on this regex pattern.")
    ValueProvider<String> getFamilyNameRegexFilter();

    @SuppressWarnings("unused")
    void setFamilyNameRegexFilter(ValueProvider<String> columnFamilyFilter);

    @Description("Filter cells by BigTable column qualifier based on this regex pattern.")
    ValueProvider<String> getColumnQualifierRegexFilter();

    @SuppressWarnings("unused")
    void setColumnQualifierRegexFilter(ValueProvider<String> columnQualifierFilter);

    @Validation.Required
    @Description("The BigQuery table spec to write the output to")
    ValueProvider<String> getOutputTableSpec();

    @SuppressWarnings("unused")
    void setOutputTableSpec(ValueProvider<String> value);

    @Validation.Required
    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    @SuppressWarnings("unused")
    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
  }

  /**
   * Runs a pipeline to export data from a Cloud Bigtable table to a BigQuery table.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    PipelineResult result = run(options);

    // Wait for pipeline to finish only if it is not constructing a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    // Build BigTable rowFilter based on Pipeline Options input
    RowFilter.Builder rowFilter = RowFilter.newBuilder();

    // Filter BigTable cells on Timestamp if start and/or end times are specified.
    TimestampRange.Builder timestampFilter = TimestampRange.newBuilder();
    if (options.getBigtableStartTime().isAccessible()) {
      timestampFilter.setStartTimestampMicros(
          NestedValueProvider.of(options.getBigtableStartTime(), new ParseDateTime()).get());
    }
    if (options.getBigtableEndTime().isAccessible()) {
      timestampFilter.setEndTimestampMicros(
          NestedValueProvider.of(options.getBigtableEndTime(), new ParseDateTime()).get());
    }
    rowFilter.setTimestampRangeFilter(timestampFilter.build());

    // Filter BigTable cells on Column Family / Qualifier if specified.
    if (options.getFamilyNameRegexFilter().isAccessible()) {
      rowFilter.setFamilyNameRegexFilter(options.getFamilyNameRegexFilter().get());
    }
    if (options.getColumnQualifierRegexFilter().isAccessible()) {
      rowFilter.setColumnQualifierRegexFilter(
          NestedValueProvider.of(
                  options.getColumnQualifierRegexFilter(),
                  new SimpleFunction<String, ByteString>() {
                    @Override
                    public ByteString apply(String input) {
                      return ByteString.copyFromUtf8(input);
                    }
                  })
              .get());
    }

    // Create Read/Write IO.
    BigtableIO.Read read =
        BigtableIO.read()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId())
            .withRowFilter(rowFilter.build());

    BigQueryIO.Write write =
        BigQueryIO.writeTableRows()
            .withSchema(SCHEMA)
            .to(options.getOutputTableSpec())
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
            .optimizedWrites();

    // Build pipeline.
    pipeline
        .apply("Read from Bigtable", read)
        .apply("Transform to TableRow", MapElements.via(new BigtableToTableRow()))
        .apply("Add Metadata", new AddMetadata(LocalDateTime.now()))
        .apply("Write to BigQuery", write);

    return pipeline.run();
  }

  /** Translates Bigtable {@link Row} to BigQuery {@link TableRow}. */
  static class BigtableToTableRow extends SimpleFunction<Row, TableRow> {
    @Override
    public TableRow apply(Row row) {
      ByteBuffer key = ByteBuffer.wrap(BytesUtils.toByteArray(row.getKey()));
      List<TableRow> cells = new ArrayList<>();
      for (Family family : row.getFamiliesList()) {
        String familyName = family.getName();
        for (Column column : family.getColumnsList()) {
          ByteBuffer qualifier = ByteBuffer.wrap(BytesUtils.toByteArray(column.getQualifier()));
          for (Cell cell : column.getCellsList()) {
            Instant timestamp =
                Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(cell.getTimestampMicros()));
            ByteBuffer value = ByteBuffer.wrap(BytesUtils.toByteArray(cell.getValue()));
            cells.add(
                new TableRow()
                    .set("family", familyName)
                    .set("qualifier", qualifier)
                    .set("timestamp", DATETIME_FORMATTER.format(timestamp))
                    .set("value", value));
          }
        }
      }
      return new TableRow().set("key", key).set("cells", cells);
    }
  }

  /** Add additional metadata to the BigQuery {@link TableRow}. */
  static class AddMetadata extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
    LocalDateTime loadTime;

    public AddMetadata(LocalDateTime loadTime) {
      this.loadTime = loadTime;
    }

    @Override
    public PCollection<TableRow> expand(PCollection<TableRow> rows) {
      // Add load time to TableRow
      return rows.apply(
          ParDo.of(
              new DoFn<TableRow, TableRow>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  c.output(c.element().set("load_time", DATETIME_FORMATTER.format(loadTime)));
                }
              }));
    }
  }

  /** Parse input datetime string to epoch milliseconds. */
  static class ParseDateTime extends SimpleFunction<String, Long> {
    @Override
    public Long apply(String input) {
      return LocalDateTime.parse(input, DateTimeFormatter.ISO_DATE_TIME)
          .toInstant(ZoneOffset.UTC)
          .toEpochMilli();
    }
  }
}
