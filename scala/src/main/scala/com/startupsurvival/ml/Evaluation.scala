package com.startupsurvival.ml

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.rdd.RDD
import java.io.{File, PrintWriter}
import java.time.LocalDate
import com.startupsurvival.spark.{StartupRDD, NewsRDD, Aggregation, FeatureJoin, FeatureSets}

/**
 * PHASE 18 - Evaluation
 *
 * Computes accuracy, precision, recall, F1, AUC-ROC, and the confusion
 * matrix for a set of (prediction, label) pairs, and runs all four
 * experiments (Phases 16-17) end to end, writing a single comparison
 * table to results/experiment_results.csv for Phase 19's visualizations
 * to consume.
 *
 * Uses org.apache.spark.mllib.evaluation - the older RDD-based metrics
 * API is deliberately kept here rather than reimplemented, since Spark's
 * newer DataFrame-based MulticlassClassificationEvaluator only exposes
 * ONE metric per call and has no confusion-matrix accessor at all; the
 * RDD-based MulticlassMetrics gives accuracy/precision/recall/F1/
 * confusion-matrix from a single object built once, which is both less
 * code and avoids recomputing predictions four times per experiment.
 *
 * IMPORTANT - precision/recall/F1 here are for the POSITIVE class
 * (label=1, "operating"/survived), matching the project brief's framing
 * of this as a survival-prediction task. Given Phase 6's real class
 * distribution (label=0 -> 2902, label=1 -> 19173, roughly 13%/87%),
 * accuracy alone is a weak metric - a model that always predicts "1"
 * would score ~87% accuracy while being useless. Precision/recall/F1 on
 * the minority class (label=0, "closed") are reported alongside for
 * exactly this reason and should be weighted more heavily in the
 * report's discussion than raw accuracy.
 */
object Evaluation {

  final case class Metrics(
      experimentName: String,
      modelType: String,
      featureSet: String,
      testCount: Long,
      accuracy: Double,
      precisionLabel1: Double,
      recallLabel1: Double,
      f1Label1: Double,
      precisionLabel0: Double,
      recallLabel0: Double,
      f1Label0: Double,
      auc: Double,
      truePositive: Double,
      trueNegative: Double,
      falsePositive: Double,
      falseNegative: Double
  )

  /**
   * predictions: a DataFrame with `label` and `prediction` Double columns
   * (produced identically by both LogisticRegressionModel.transform and
   * LinearSVCModel.transform), plus a `rawPrediction` vector column used
   * only for the AUC calculation.
   */
  def evaluate(
      predictions: DataFrame,
      experimentName: String,
      modelType: String,
      featureSet: String
  ): Metrics = {
    import predictions.sparkSession.implicits._

    val predictionAndLabels: RDD[(Double, Double)] =
      predictions.select("prediction", "label").as[(Double, Double)].rdd

    val multiclass = new MulticlassMetrics(predictionAndLabels)
    val confusion = multiclass.confusionMatrix // rows/cols ordered by label: [0][0]=TN(for 0-as-positive-view) etc.

    // MulticlassMetrics indexes classes by their label VALUE, so label=1
    // is index 1, label=0 is index 0 - confirmed by construction, not
    // assumed, since these are the only two label values Phase 2's R2
    // rule ever produces.
    val precisionLabel1 = multiclass.precision(1.0)
    val recallLabel1 = multiclass.recall(1.0)
    val f1Label1 = multiclass.fMeasure(1.0)
    val precisionLabel0 = multiclass.precision(0.0)
    val recallLabel0 = multiclass.recall(0.0)
    val f1Label0 = multiclass.fMeasure(0.0)
    val accuracy = multiclass.accuracy

    // Confusion matrix entries, treating label=1 ("operating") as the
    // positive class per the project brief's survival-prediction framing.
    val tp = confusion(1, 1)
    val tn = confusion(0, 0)
    val fp = confusion(0, 1) // predicted 1, actually 0
    val fn = confusion(1, 0) // predicted 0, actually 1

    // AUC-ROC needs a real-valued score, not the discrete 0/1 prediction
    // - both LogisticRegressionModel and LinearSVCModel expose
    // `rawPrediction` (a 2-vector; index 1 is the score for the positive
    // class) for exactly this purpose.
    val scoreAndLabels: RDD[(Double, Double)] =
      predictions.select("rawPrediction", "label").rdd.map { row =>
        val raw = row.getAs[org.apache.spark.ml.linalg.Vector](0)
        (raw(1), row.getDouble(1))
      }
    val auc = new BinaryClassificationMetrics(scoreAndLabels).areaUnderROC()

    Metrics(
      experimentName, modelType, featureSet, predictions.count(),
      accuracy, precisionLabel1, recallLabel1, f1Label1,
      precisionLabel0, recallLabel0, f1Label0, auc,
      tp, tn, fp, fn
    )
  }

  def printMetrics(m: Metrics): Unit = {
    println(s"--- ${m.experimentName} (${m.modelType}, ${m.featureSet}) ---")
    println(f"Test set size: ${m.testCount}")
    println(f"Accuracy: ${m.accuracy}%.4f")
    println(f"AUC-ROC: ${m.auc}%.4f")
    println(f"Label=1 (operating) - Precision: ${m.precisionLabel1}%.4f  Recall: ${m.recallLabel1}%.4f  F1: ${m.f1Label1}%.4f")
    println(f"Label=0 (closed)    - Precision: ${m.precisionLabel0}%.4f  Recall: ${m.recallLabel0}%.4f  F1: ${m.f1Label0}%.4f")
    println(f"Confusion matrix: TP=${m.truePositive}%.0f  TN=${m.trueNegative}%.0f  FP=${m.falsePositive}%.0f  FN=${m.falseNegative}%.0f")
    println()
  }

  /** Writes the comparison table Phase 19's visualizations read. Path is
   * relative to wherever `sbt run` is invoked from
   * (~/startup-survival-prediction/scala per every prior phase's "What
   * to run" instructions), so ../results/ lands at the project's actual
   * results/ folder, not a stray one inside scala/. */
  def writeCsv(allMetrics: Seq[Metrics], path: String = "../results/experiment_results.csv"): Unit = {
    val file = new File(path)
    Option(file.getParentFile).foreach(_.mkdirs())
    val writer = new PrintWriter(file)
    try {
      writer.println("experiment,model,feature_set,test_count,accuracy,auc,precision_label1,recall_label1,f1_label1,precision_label0,recall_label0,f1_label0,tp,tn,fp,fn")
      allMetrics.foreach { m =>
        writer.println(Seq(
          m.experimentName, m.modelType, m.featureSet, m.testCount,
          m.accuracy, m.auc, m.precisionLabel1, m.recallLabel1, m.f1Label1,
          m.precisionLabel0, m.recallLabel0, m.f1Label0,
          m.truePositive, m.trueNegative, m.falsePositive, m.falseNegative
        ).mkString(","))
      }
    } finally {
      writer.close()
    }
    println(s"Wrote ${allMetrics.length} experiment results to $path")
  }

  /** Writes the two Logistic Regression models' REAL trained coefficients
   * and intercepts to JSON, so Phase 20's dashboard can run live
   * predictions for a new startup client-side, in-browser, using the
   * actual trained weights - not a separately-fabricated "demo" model.
   * SVM is not exported here: LinearSVC's rawPrediction is a signed
   * distance, not a calibrated probability, and a live "here is your
   * risk" number needs LR's probability output specifically (see the
   * class-level note in SVM.scala on why the two model types aren't
   * directly comparable on this dimension). */
  def writeModelCoefficients(
      financialOnlyModel: org.apache.spark.ml.classification.LogisticRegressionModel,
      financialSentimentModel: org.apache.spark.ml.classification.LogisticRegressionModel,
      path: String = "../results/model_coefficients.json"
  ): Unit = {
    def modelJson(model: org.apache.spark.ml.classification.LogisticRegressionModel, featureNames: Array[String]): String = {
      val coeffs = model.coefficients.toArray
      val coeffsJson = featureNames.zip(coeffs)
        .map { case (name, c) => s"""{"name":"$name","coefficient":$c}""" }
        .mkString("[", ",", "]")
      s"""{"intercept":${model.intercept},"features":$coeffsJson}"""
    }

    val json =
      s"""{
         |  "generatedNote": "Real trained Logistic Regression coefficients from Phase 16, exported by Phase 18's Evaluation.scala. Used by the Phase 20 dashboard for live in-browser predictions - not a separate or fabricated model.",
         |  "financialOnly": ${modelJson(financialOnlyModel, FeatureSets.FinancialFeatureNames)},
         |  "financialSentiment": ${modelJson(financialSentimentModel, FeatureSets.FinancialFeatureNames ++ FeatureSets.SentimentFeatureNames)}
         |}
         |""".stripMargin

    val file = new File(path)
    Option(file.getParentFile).foreach(_.mkdirs())
    val writer = new PrintWriter(file)
    try { writer.print(json) } finally { writer.close() }
    println(s"Wrote real trained LR coefficients to $path (for the live dashboard predictor)")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Evaluation-Phase18")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cutoff = LocalDate.of(2013, 1, 1)

    println("=== Rebuilding the full pipeline (Phases 12-17) ===")
    val startupRDD = StartupRDD.load(spark)
    val financialRDD = StartupRDD.extractFinancialFeaturesRDD(startupRDD)
    val newsRDD = NewsRDD.load(spark)
    val sentimentRDD = Aggregation.toSentimentFeaturesRDD(newsRDD.groupByKey(), cutoff)
    val combinedRDD = FeatureJoin.join(financialRDD, sentimentRDD, cutoff)

    val financialOnlyDF = FeatureSets.financialOnly(spark, combinedRDD)
    val financialSentimentDF = FeatureSets.financialPlusSentiment(spark, combinedRDD)

    println("\n=== Training all 4 experiments ===")
    val exp1 = LogisticRegression.trainAndPredict(financialOnlyDF)
    val exp2 = SVM.trainAndPredict(financialOnlyDF)
    val exp3 = LogisticRegression.trainAndPredict(financialSentimentDF)
    val exp4 = SVM.trainAndPredict(financialSentimentDF)

    println("\n=== Evaluating all 4 experiments ===")
    val m1 = evaluate(exp1.predictions, "Experiment 1", "Logistic Regression", "Financial-only")
    val m2 = evaluate(exp2.predictions, "Experiment 2", "Binary SVM", "Financial-only")
    val m3 = evaluate(exp3.predictions, "Experiment 3", "Logistic Regression", "Financial+Sentiment")
    val m4 = evaluate(exp4.predictions, "Experiment 4", "Binary SVM", "Financial+Sentiment")

    val allMetrics = Seq(m1, m2, m3, m4)
    allMetrics.foreach(printMetrics)

    println("=== Summary comparison ===")
    println(f"${"Experiment"}%-14s ${"Model"}%-22s ${"Features"}%-22s ${"Accuracy"}%-10s ${"AUC"}%-8s ${"F1(label=0)"}%-12s")
    allMetrics.foreach { m =>
      println(f"${m.experimentName}%-14s ${m.modelType}%-22s ${m.featureSet}%-22s ${m.accuracy}%-10.4f ${m.auc}%-8.4f ${m.f1Label0}%-12.4f")
    }
    println("\nCompare 1 vs 3, and 2 vs 4: does adding sentiment features improve")
    println("F1(label=0)/AUC over the financial-only baseline? That comparison is this")
    println("project's actual research question - state it explicitly in the report,")
    println("not just which single experiment had the highest raw accuracy.")

    writeCsv(allMetrics)
    writeModelCoefficients(exp1.model, exp3.model)

    spark.stop()
  }
}
