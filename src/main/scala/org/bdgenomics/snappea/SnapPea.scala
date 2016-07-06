/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
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
package org.bdgenomics.snappea

import org.apache.spark.SparkContext._
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

private[snappea] object SnapPea extends BDGCommandCompanion {
  val commandName = "snappea"
  val commandDescription = "Parallel alignment using SNAP on ADAM/Spark."

  def apply(cmdLine: Array[String]) = {
    new SnapPea(Args4j[SnapPeaArgs](cmdLine))
  }
}

private[snappea] class SnapPeaArgs extends Args4jBase with ParquetArgs with SaveArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The reads to align.", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "The place to save alignments.", index = 1)
  var outputPath: String = null

  @Argument(required = true, metaVar = "REFERENCE", usage = "The reference genome.", index = 2)
  var referencePath: String = null
}

private[snappea] class SnapPea(protected val args: SnapPeaArgs) extends BDGSparkCommand[SnapPeaArgs] {
  val companion = SnapPea

  def run(sc: SparkContext) {

    // load in the reference genome
    val reference: RDD[NucleotideContigFragment] = sc.loadSequence(args.referencePath)

    // align the reads
    val alignedReads = AlignReads(sc,
      args.inputPath,
      null,
      reference)

    // save the reads
    alignedReads.adamParquetSave(args)
  }
}
