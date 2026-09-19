package com.startupsurvival.ml

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.classification.{LogisticRegression => SparkLogisticRegression, LogisticRegressionModel}
import java.time.LocalDate
import com.startupsurvival.spark.{StartupRDD, NewsRDD, Aggregation, FeatureJoin, FeatureSets}

/**
 * PHASE 16 - Logistic Regression (Spark MLlib)
 *
 * Per docs/PHASE00_scope.md's fixed architecture (Spark MLlib for LR/SVM,
 * demonstrating Unit 3's "MLlib, SVM, Logistic Regression" syllabus
 * points) and docs/PHASE03_schema_architecture.md's interface contract -
 * this deliberately uses org.apache.spark.ml.classification's
 * DataFrame-based API (spark.ml), not a hand-rolled gradient-descent
 * implementation. Phases 12-13 already demonstrate raw RDD mechanics
 * (map/reduceByKey/groupByKey/join) for Unit 3's RDD-specific points;
 * this phase demonstrates the library-based ML side of the same unit.
 *
 * Feeds Experiment 1 (financial-only + LR, Phase 14's feature set) and
 * Experiment 3 (financial+sentiment + LR, Phase 15's feature set) - the
 * SAME trainAndPredict function runs both, since the only difference
 * between the two experiments is which DataFrame is passed in, not the
 * model logic itself.
 */
object LogisticRegression {

  /** Fixed everywhere a train/test split happens in this project, so
   * Experiments 1-4 are all compared on the same random partition of
   * startups rather than each experiment silently drawing a different
   * split - a fair-comparison requirement, not an arbitrary choice. */
  val RandomSeed: Long = 42L
  val TrainFraction: Double = 0.8

  final case class TrainedModel(
      model: LogisticRegressionModel,
      predictions: DataFrame, // columns: permalink, label, features, rawPrediction, probability, prediction
      trainCount: Long,
      testCount: Long
  )

  /**
   * Splits featureDF (permalink, label, features) into train/test with
   * the fixed seed/fraction above, fits a Spark ML LogisticRegression,
   * and returns predictions on the held-out test set only - never
   * reporting metrics computed on the training data itself, which would
   * overstate real performance.
   */
  def trainAndPredict(featureDF: DataFrame): TrainedModel = {
    val Array(trainDF, testDF) = featureDF.randomSplit(Array(TrainFraction, 1 - TrainFraction), seed = RandomSeed)
    val weightedTrainDF = ClassWeights.addBalancedWeights(trainDF).persist()
    testDF.persist()

    val lr = new SparkLogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setWeightCol("weight") // PHASE 16/17 FIX - see ClassWeights.scala. Without this, the
                                // ~13%/87% class split let the model reach high accuracy by
                                // effectively ignoring the minority (closed) class - confirmed
                                // by Phase 18's first real run (label=0 recall was 0.0018).
      .setMaxIter(100)
      .setRegParam(0.01)        // small L2 penalty - guards against the high-cardinality-adjacent
                                  // funding features overfitting on a training set this size
      .setElasticNetParam(0.0)  // pure L2 (ridge), not L1 - no feature-selection motivation here,
                                  // all financial/sentiment features were deliberately engineered,
                                  // not dumped in raw, so there is nothing to sparsify away

    val model = lr.fit(weightedTrainDF)
    val predictions = model.transform(testDF)

    TrainedModel(model, predictions, weightedTrainDF.count(), testDF.count())
  }

  /** Prints the model's learned coefficients paired with their feature
   * names (Phase 14/15's fixed column order), so the report/viva can
   * discuss which features drove predictions - not just report an
   * opaque accuracy number. */
  def printCoefficients(result: TrainedModel, featureNames: Array[String]): Unit = {
    val coeffs = result.model.coefficients.toArray
    println(s"Intercept: ${result.model.intercept}")
    println("Coefficients:")
    featureNames.zip(coeffs).foreach { case (name, coeff) =>
      println(f"  $name%-28s $coeff%+.6f")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LogisticRegression-Phase16")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cutoff = LocalDate.of(2013, 1, 1)

    println("=== Rebuilding Phase 13's combined RDD and Phase 14/15's feature sets ===")
    val startupRDD = StartupRDD.load(spark)
    val financialRDD = StartupRDD.extractFinancialFeaturesRDD(startupRDD)
    val newsRDD = NewsRDD.load(spark)
    val sentimentRDD = Aggregation.toSentimentFeaturesRDD(newsRDD.groupByKey(), cutoff)
    val combinedRDD = FeatureJoin.join(financialRDD, sentimentRDD, cutoff)

    val financialOnlyDF = FeatureSets.financialOnly(spark, combinedRDD)
    val financialSentimentDF = FeatureSets.financialPlusSentiment(spark, combinedRDD)

    println("\n=== EXPERIMENT 1: Financial-only + Logistic Regression ===")
    val exp1 = trainAndPredict(financialOnlyDF)
    println(s"Train rows: ${exp1.trainCount}, test rows: ${exp1.testCount}")
    printCoefficients(exp1, FeatureSets.FinancialFeatureNames)
    exp1.predictions.select("permalink", "label", "prediction", "probability").show(10, truncate = false)

    println("\n=== EXPERIMENT 3: Financial + Sentiment + Logistic Regression ===")
    val exp3 = trainAndPredict(financialSentimentDF)
    println(s"Train rows: ${exp3.trainCount}, test rows: ${exp3.testCount}")
    printCoefficients(exp3, FeatureSets.FinancialFeatureNames ++ FeatureSets.SentimentFeatureNames)
    exp3.predictions.select("permalink", "label", "prediction", "probability").show(10, truncate = false)

    println("\nRun Evaluation.scala (Phase 18) against these predictions for accuracy/precision/")
    println("recall/F1/confusion-matrix numbers - this file deliberately stops at raw")
    println("predictions, not metrics, to keep evaluation logic in one place shared by")
    println("both LogisticRegression.scala and SVM.scala.")

    spark.stop()
  }
}
