/*
 * Copyright 2019 The Glow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.projectglow.bgen

import java.util.{HashMap => JHashMap}

import org.apache.commons.math3.util.CombinatoricsUtils

// Tools for calculating ploidy or number of genotypes for unphased posterior probabilities
private[projectglow] object BgenConverterUtils {
  var ploidyMap = new JHashMap[(Int, Int), Int] // (numGenotypes, numAlleles) to ploidy
  var genotypesMap = new JHashMap[(Int, Int), Int] // (ploidy, numAlleles) to numGenotypes

  def getPloidy(numGenotypes: Int, numAlleles: Int, maxPloidy: Int): Int = {
    if (ploidyMap.containsKey((numGenotypes, numAlleles))) {
      ploidyMap.get((numGenotypes, numAlleles))
    } else {
      var possibleNumGenotypes = 0
      var possiblePloidy = 0

      while (possibleNumGenotypes < numGenotypes && possiblePloidy < maxPloidy) {
        possiblePloidy += 1
        possibleNumGenotypes = CombinatoricsUtils
          .binomialCoefficient(
            possiblePloidy + numAlleles - 1,
            numAlleles - 1
          )
          .toInt
        ploidyMap.put((possibleNumGenotypes, numAlleles), possiblePloidy)
      }
      if (numGenotypes != possibleNumGenotypes) {
        throw new IllegalStateException(
          s"Ploidy cannot be inferred: unphased, $numAlleles alleles, $maxPloidy maximum ploidy, " +
          s"$numGenotypes probabilities."
        )
      }
      possiblePloidy
    }
  }

  def getNumGenotypes(ploidy: Int, numAlleles: Int): Int = {
    if (genotypesMap.containsKey((ploidy, numAlleles))) {
      genotypesMap.get((ploidy, numAlleles))
    } else {
      val numGts = CombinatoricsUtils
        .binomialCoefficient(
          ploidy + numAlleles - 1,
          numAlleles - 1
        )
        .toInt
      genotypesMap.put((ploidy, numAlleles), numGts)
      numGts
    }
  }
}
