package io.projectglow.tertiary

import breeze.linalg.{DenseMatrix, DenseVector}
import io.projectglow.sql.GlowBaseTest
import io.projectglow.sql.expressions.NewtonIterationsState

class NewtonIterationsStateSuite extends GlowBaseTest {
  test("initializes beta correctly") {
    val rows = 10
    val cols = 11
    val fitState = new NewtonIterationsState(rows, cols)
    val X = DenseMatrix.ones[Double](rows, cols)
    val Y = DenseVector.ones[Double](rows)
    val nullFit = new NewtonIterationsState(rows, cols - 1)
    nullFit.b := 1d
    fitState.b := 2d
    fitState.initFromMatrixAndNullFit(X, Y, None, nullFit)

    assert(fitState.b(0 to -2) == DenseVector.ones[Double](cols - 1))
    // the last element in beta should be set to 0 in initialization
    assert(fitState.b(-1) == 0d)
  }
}
