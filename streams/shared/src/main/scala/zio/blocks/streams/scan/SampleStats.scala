/*
 * Copyright 2024-2026 John A. De Goes and the ZIO Contributors
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

package zio.blocks.streams.scan

/**
 * Online statistics over a stream of `Double` observations.
 *
 * Tracks the size and the first four central moments. Forms a Monoid under
 * [[merge]] with [[SampleStats.empty]] as the identity (Chan parallel
 * algorithm). Single-step updates via [[observe]] use Welford's algorithm.
 *
 * Numerically stable; suitable for streaming aggregation over arbitrary
 * windows. Modelled on Quasar's `SampleStats` (Apache 2.0; see
 * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance).
 *
 * Generic-over-`A: Field[A]` is a follow-up; v1 specialises to `Double` to
 * keep the streams module Spire-free (zero deps).
 *
 * @param size
 *   Number of observations.
 * @param m1
 *   First central moment (the mean when `size > 0`).
 * @param m2
 *   Sum of squared deviations from the mean.
 * @param m3
 *   Third central moment (related to skewness).
 * @param m4
 *   Fourth central moment (related to kurtosis).
 *
 * @example
 *   {{{
 *   val s = SampleStats.empty.observe(1.0).observe(2.0).observe(3.0)
 *   s.mean              // 2.0
 *   s.populationStddev  // Some(1.0)
 *   s.merge(s) == s + s // true
 *   }}}
 */
final case class SampleStats(
    size: Long,
    m1: Double,
    m2: Double,
    m3: Double,
    m4: Double
) {

  // ---------------------------------------------------------------------------
  //  Sample statistics (treat the observations themselves as the universe)
  // ---------------------------------------------------------------------------

  /** Arithmetic mean. Defined as 0.0 when `size == 0` (matches `m1`). */
  def mean: Double = m1

  /** Sample variance: `m2 / size`. `None` when no observations. */
  def variance: Option[Double] =
    if (size == 0L) None else Some(m2 / size.toDouble)

  /** Sample standard deviation. */
  def stddev: Option[Double] = variance.map(math.sqrt)

  /**
   * Sample skewness (third standardised moment). Defined when `size > 0` and
   * `m2 != 0`.
   */
  def skewness: Option[Double] =
    if (m2 == 0.0 || size == 0L) None
    else Some((math.sqrt(size.toDouble) * m3) / math.pow(m2, 1.5))

  /**
   * Sample kurtosis (fourth standardised moment, NOT excess kurtosis). The
   * Gaussian distribution has kurtosis 3.
   */
  def kurtosis: Option[Double] =
    if (m2 == 0.0) None else Some((size.toDouble * m4) / (m2 * m2))

  /** Excess kurtosis: kurtosis minus 3. Zero for the Gaussian. */
  def excessKurtosis: Option[Double] = kurtosis.map(_ - 3.0)

  // ---------------------------------------------------------------------------
  //  Population estimates (treat the observations as a sample of a larger
  //  population; estimators are unbiased / consistent up to the standard
  //  caveats).
  // ---------------------------------------------------------------------------

  /** Unbiased estimate of population variance. Requires `size > 1`. */
  def populationVariance: Option[Double] =
    if (size <= 1L) None else Some(m2 / (size.toDouble - 1.0))

  /** Estimated population standard deviation. */
  def populationStddev: Option[Double] = populationVariance.map(math.sqrt)

  /**
   * Estimated population skewness via the standard `g1` adjustment.
   * Requires `size > 2` and `m2 != 0`.
   */
  def populationSkewness: Option[Double] =
    if (m2 == 0.0 || size <= 2L) None
    else {
      val n = size.toDouble
      Some((n * math.sqrt(n - 1.0) * m3) / ((n - 2.0) * math.pow(m2, 1.5)))
    }

  /**
   * Estimated population kurtosis via the standard `g2` adjustment.
   * Requires `size > 3` and `m2 != 0`.
   */
  def populationKurtosis: Option[Double] = {
    val n     = size.toDouble
    val denom = (n - 2.0) * (n - 3.0) * m2 * m2
    if (denom == 0.0 || size <= 3L) None
    else Some((n * (n + 1.0) * (n - 1.0) * m4) / denom)
  }

  /** Estimated population excess kurtosis. */
  def populationExcessKurtosis: Option[Double] = populationKurtosis.map(_ - 3.0)

  // ---------------------------------------------------------------------------
  //  Operations
  // ---------------------------------------------------------------------------

  /**
   * Returns new stats including the given observation.
   *
   * Implementation: Welford's recurrence (numerically stable).
   */
  def observe(x: Double): SampleStats = {
    val n        = (size + 1L).toDouble
    val delta    = x - m1
    val deltaN   = delta / n
    val deltaN2  = deltaN * deltaN
    val term1    = delta * deltaN * size.toDouble
    SampleStats(
      size + 1L,
      m1 + deltaN,
      m2 + term1,
      m3 + (term1 * deltaN * (n - 2.0)) - (3.0 * deltaN * m2),
      m4 + (term1 * deltaN2 * ((n * n) - (3.0 * n) + 3.0)) +
        (6.0 * deltaN2 * m2) -
        (4.0 * deltaN * m3)
    )
  }

  /**
   * Combine with another `SampleStats` to produce stats about the union of
   * their observations. Implementation via the Chan parallel algorithm;
   * forms a commutative monoid with [[SampleStats.empty]] as identity.
   */
  def merge(b: SampleStats): SampleStats =
    if (b.size == 0L) this
    else if (size == 0L) b
    else if (b.size == 1L) observe(b.mean)
    else if (size == 1L) b.observe(mean)
    else {
      val n1   = size.toDouble
      val n1sq = n1 * n1
      val n2   = b.size.toDouble
      val n2sq = n2 * n2
      val n12  = n1 * n2
      val n    = n1 + n2
      val nsq  = n * n
      val ncb  = nsq * n
      val d    = b.m1 - m1
      val d2   = d * d
      val d3   = d2 * d
      val d4   = d2 * d2
      SampleStats(
        size + b.size,
        ((n1 * m1) + (n2 * b.m1)) / n,
        m2 + b.m2 + ((d2 * n12) / n),
        m3 + b.m3 + ((d3 * n12 * (n1 - n2)) / nsq) + ((3.0 * d * ((n1 * b.m2) - (n2 * m2))) / n),
        m4 + b.m4 + ((d4 * n12 * (n1sq - n12 + n2sq)) / ncb) +
          ((6.0 * d2 * ((n1sq * b.m2) + (n2sq * m2))) / nsq) +
          ((4.0 * d * ((n1 * b.m3) - (n2 * m3))) / n)
      )
    }

  /** Alias for [[merge]]. */
  def +(b: SampleStats): SampleStats = merge(b)
}

/**
 * Companion for [[SampleStats]]. Provides the empty / single-observation
 * constructors and an `Iterable[Double]` reducer.
 */
object SampleStats {

  /** Stats over zero observations. Identity element of [[SampleStats.merge]]. */
  val empty: SampleStats = SampleStats(0L, 0.0, 0.0, 0.0, 0.0)

  /** Stats over the frequency of an observation. */
  def freq(count: Long, a: Double): SampleStats = SampleStats(count, a, 0.0, 0.0, 0.0)

  /** Stats over a single observation. */
  def one(a: Double): SampleStats = freq(1L, a)

  /** Stats reduced from any `Iterable[Double]` via `observe`. */
  def fromIterable(xs: Iterable[Double]): SampleStats =
    xs.foldLeft(empty)(_ observe _)
}
