/*
 * Copyright (C) 2018 Google Inc.
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

import static com.google.cloud.teleport.bigtable.BigtableToBigQuery.BigtableToTableRow;
import static com.google.cloud.teleport.bigtable.TestUtils.createBigtableRow;
import static com.google.cloud.teleport.bigtable.TestUtils.createTableRow;
import static com.google.cloud.teleport.bigtable.TestUtils.createTableRowCell;
import static com.google.cloud.teleport.bigtable.TestUtils.toByteBuffer;
import static com.google.cloud.teleport.bigtable.TestUtils.upsertBigtableCell;

import com.google.api.services.bigquery.model.TableRow;
import com.google.bigtable.v2.Row;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for BigtableToAvro. */
@RunWith(JUnit4.class)
public final class BigtableToBigQueryTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void applyBigtableToAvroFn() throws Exception {
    Row bigtableRow1 = createBigtableRow("row1");
    bigtableRow1 = upsertBigtableCell(bigtableRow1, "family1", "column1", 1000000, "value1");
    bigtableRow1 = upsertBigtableCell(bigtableRow1, "family1", "column1", 2000000, "value2");
    bigtableRow1 = upsertBigtableCell(bigtableRow1, "family1", "column2", 1000000, "value3");
    bigtableRow1 = upsertBigtableCell(bigtableRow1, "family2", "column1", 1000000, "value4");
    Row bigtableRow2 = createBigtableRow("row2");
    bigtableRow2 = upsertBigtableCell(bigtableRow2, "family2", "column2", 1000000, "value2");
    final List<Row> bigtableRows = ImmutableList.of(bigtableRow1, bigtableRow2);

    TableRow tr1 =
        createTableRow(toByteBuffer("row1"))
            .set(
                "cells",
                ImmutableList.of(
                    createTableRowCell(
                        "family1",
                        toByteBuffer("column1"),
                        "1970-01-01 00:00:01.000000",
                        toByteBuffer("value1")),
                    createTableRowCell(
                        "family1",
                        toByteBuffer("column1"),
                        "1970-01-01 00:00:02.000000",
                        toByteBuffer("value2")),
                    createTableRowCell(
                        "family1",
                        toByteBuffer("column2"),
                        "1970-01-01 00:00:01.000000",
                        toByteBuffer("value3")),
                    createTableRowCell(
                        "family2",
                        toByteBuffer("column1"),
                        "1970-01-01 00:00:01.000000",
                        toByteBuffer("value4"))));
    TableRow tr2 =
        createTableRow(toByteBuffer("row2"))
            .set(
                "cells",
                ImmutableList.of(
                    createTableRowCell(
                        "family2",
                        toByteBuffer("column2"),
                        "1970-01-01 00:00:01.000000",
                        toByteBuffer("value2"))));
    final List<TableRow> expectedTableRows = ImmutableList.of(tr1, tr2);

    PCollection<TableRow> tableRows =
        pipeline
            .apply("Create", Create.of(bigtableRows))
            .apply("Transform to TableRow", MapElements.via(new BigtableToTableRow()));

    PAssert.that(tableRows).containsInAnyOrder(expectedTableRows);
    pipeline.run();
  }
}
