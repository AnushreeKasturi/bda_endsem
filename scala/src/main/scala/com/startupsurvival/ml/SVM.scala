package com.startupsurvival.ml

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import java.time.LocalDate
import com.startupsurvival.spark.{StartupRDD, NewsRDD, Aggregation, FeatureJoin, FeatureSets}

/**
 * PHASE 17 - Binary SVM (Spark MLlib)
 *
 * Uses org.apache.spark.ml.classification.LinearSVC - the current
 * DataFrame-based Spark ML linear SVM implementation (the older
 * RDD-based org.apache.spark.mllib.classification.SVMWithSGD is legacy
 * API; LinearSVC is what current Spark documentation and MLlib itself
 * point to for this task, and it shares the same feature/label DataFrame
 * contract as LogisticRegression.scala, Phase 16, keeping both
 * experiments' code symmetric).
 *
 * NOTE: LinearSVC's `rawPrediction` is a signed distance from the
 * separating hyperplane, NOT a calibrated probability the way Logistic
 * Regression's `probability` column is - SVMs are margin classifiers,
 * not probabilistic ones by construction. Evaluation.scala (Phase 18)
 * accounts for this: it reads `prediction` (the 0/1 class label both
 * models expose identically) for accuracy/precision/recall/F1/confusion
 * matrix, and does not attempt to compare probability calibration
 * between the two model types, which would not be a like-for-like
 * comparison.
 *
 * Feeds Experiment 2 (financial-only + SVM) and Experiment 4
 * (financial+sentiment + SVM).
 */
object SVM {

  final case class TrainedModel(
      model: LinearSVCModel,
      predictions: DataFrame, // columns: permalink, label, features, rawPrediction, prediction
      trainCount: Long,
      testCount: Long
  )

  /** Same fixed seed/split as LogisticRegression.scala (Phase 16) - see
   * that file's comment on why this must be shared, not independently
   * chosen per model. */
  def trainAndPredict(featureDF: DataFrame): TrainedModel = {
    val Array(trainDF, testDF) = featureDF.randomSplit(
      Array(LogisticRegression.TrainFraction, 1 - LogisticRegression.TrainFraction),
      seed = LogisticRegression.RandomSeed
    )
    val weightedTrainDF = ClassWeights.addBalancedWeights(trainDF).persist()
    testDF.persist()

    val svm = new LinearSVC()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setWeightCol("weight") // PHASE 16/17 FIX - see ClassWeights.scala and the identical
                                // note in LogisticRegression.scala. LinearSVC has supported
                                // setWeightCol since Spark 2.2, so this is not a fallback or
                                // approximation - it uses the same real weighted-hinge-loss
                                // objective as an unweighted LinearSVC, just with per-row weights.
      .setMaxIter(100)
      .setRegParam(0.01) // same regularization strength as Phase 16's LR, for a fair
                          // apples-to-apples comparison between the two model families

    val model = svm.fit(weightedTrainDF)
    val predictions = model.transform(testDF)

    TrainedModel(model, predictions, weightedTrainDF.count(), testDF.count())
  }

  def printCoefficients(result: TrainedModel, featureNames: Array[String]): Unit = {
    val coeffs = result.model.coefficients.toArray
    println(s"Intercept: ${result.model.intercept}")
    println("Coefficients (signed distance contribution, not a probability):")
    featureNames.zip(coeffs).foreach { case (name, coeff) =>
      println(f"  $name%-28s $coeff%+.6f")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SVM-Phase17")
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

    println("\n=== EXPERIMENT 2: Financial-only + Binary SVM ===")
    val exp2 = trainAndPredict(financialOnlyDF)
    println(s"Train rows: ${exp2.trainCount}, test rows: ${exp2.testCount}")
    printCoefficients(exp2, FeatureSets.FinancialFeatureNames)
    exp2.predictions.select("permalink", "label", "prediction", "rawPrediction").show(10, truncate = false)

    println("\n=== EXPERIMENT 4: Financial + Sentiment + Binary SVM ===")
    val exp4 = trainAndPredict(financialSentimentDF)
    println(s"Train rows: ${exp4.trainCount}, test rows: ${exp4.testCount}")
    printCoefficients(exp4, FeatureSets.FinancialFeatureNames ++ FeatureSets.SentimentFeatureNames)
    exp4.predictions.select("permalink", "label", "prediction", "rawPrediction").show(10, truncate = false)

    println("\nRun Evaluation.scala (Phase 18) against these predictions for accuracy/precision/")
    println("recall/F1/confusion-matrix numbers.")

    spark.stop()
  }
}
