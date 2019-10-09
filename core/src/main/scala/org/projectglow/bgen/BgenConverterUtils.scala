package org.projectglow.bgen

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
