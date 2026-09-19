package com.startupsurvival.ml

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when, lit}

/**
 * PHASE 16/17 FIX - Class weighting
 *
 * Added after Phase 18's first real evaluation run showed both models
 * essentially collapsing to "always predict label=1" - SVM's confusion
 * matrix had TN=0 (never once correctly identified a closed startup
 * across 4343 test rows), and Logistic Regression's label=0 recall was
 * 0.0018 (1 out of 557). Given the real class split confirmed back in
 * Phase 6 (label=0 -> 2902, label=1 -> 19173, ~13%/87%), an unweighted
 * classifier can reach ~87% accuracy by simply ignoring the minority
 * class entirely - which is exactly what happened. This is a genuine
 * finding from real data, not a code bug: the models trained correctly
 * on the objective they were given, and that objective (unweighted log-
 * loss/hinge-loss) does not by itself penalize ignoring the minority
 * class enough to matter at this ratio.
 *
 * Standard balanced class weighting: weight(class c) =
 * totalCount / (numClasses * countOfClass(c)) - this makes both
 * classes' TOTAL weighted contribution to the loss function equal,
 * rather than proportional to their raw frequency. Computed from the
 * TRAINING split only (never from the test set, and never from the full
 * 22,075-row population before splitting) - using test-set label
 * frequencies to influence training would leak test-set information
 * back into the model, the same category of leakage concern as Phase
 * 11's funding-data leakage guard, just at a different pipeline stage.
 */
object ClassWeights {

  /** Adds a `weight` column to a (label, features, ...) DataFrame, computed
   * from that DataFrame's OWN label distribution. Call this on the
   * training split only, after the train/test split has already happened -
   * never on the full dataset before splitting, and never on the test
   * split. */
  def addBalancedWeights(df: DataFrame): DataFrame = {
    val total = df.count()
    val positiveCount = df.filter(col("label") === 1.0).count()
    val negativeCount = total - positiveCount

    require(positiveCount > 0 && negativeCount > 0,
      s"Cannot compute balanced weights: one class is empty in this split " +
      s"(label=1 count=$positiveCount, label=0 count=$negativeCount). " +
      s"This would indicate a train/test split gone wrong, not a normal case.")

    val weightForPositive = total.toDouble / (2.0 * positiveCount)
    val weightForNegative = total.toDouble / (2.0 * negativeCount)

    println(f"  Class weights computed from this split: label=1 (n=$positiveCount) -> weight=$weightForPositive%.4f, " +
      f"label=0 (n=$negativeCount) -> weight=$weightForNegative%.4f")

    df.withColumn("weight",
      when(col("label") === 1.0, lit(weightForPositive))
        .otherwise(lit(weightForNegative))
    )
  }
}
