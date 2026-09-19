"""
PHASE 19 - Visualizations

Reads results/experiment_results.csv (written by Phase 18's
Evaluation.scala - see scala/src/main/scala/com/startupsurvival/ml/Evaluation.scala)
and produces the comparison charts for the report/presentation.

This script does NOT contain any numbers itself - it only exists to plot
whatever real numbers Evaluation.scala actually wrote after a real run.
If results/experiment_results.csv doesn't exist yet, this script fails
loudly rather than falling back to placeholder data, on purpose: a chart
of invented numbers would be worse than no chart at all for a project
built entirely around "verify against real data, never fabricate."

Run from the project root:
    python visualization/plot_results.py

Produces (all under visualization/):
    accuracy_auc_comparison.png  - grouped bar chart, all 4 experiments
    f1_by_class_comparison.png  - F1 for both classes, all 4 experiments
    confusion_matrices.png      - 2x2 grid of confusion-matrix heatmaps
    sentiment_lift.png          - Experiment 1 vs 3, and 2 vs 4, isolating
                                    the effect of adding sentiment features
"""

import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

RESULTS_CSV = os.path.join(os.path.dirname(__file__), "..", "results", "experiment_results.csv")
OUTPUT_DIR = os.path.dirname(__file__)


def load_results() -> pd.DataFrame:
    if not os.path.exists(RESULTS_CSV):
        sys.exit(
            f"ERROR: {RESULTS_CSV} not found.\n"
            "Run Phase 18 first:\n"
            "  cd ~/startup-survival-prediction/scala\n"
            '  sbt "runMain com.startupsurvival.ml.Evaluation"\n'
            "This script deliberately does not fall back to placeholder numbers -\n"
            "a chart of invented results would misrepresent the project."
        )
    df = pd.read_csv(RESULTS_CSV)
    expected_cols = {
        "experiment", "model", "feature_set", "test_count", "accuracy", "auc",
        "precision_label1", "recall_label1", "f1_label1",
        "precision_label0", "recall_label0", "f1_label0",
        "tp", "tn", "fp", "fn",
    }
    missing = expected_cols - set(df.columns)
    if missing:
        sys.exit(f"ERROR: results CSV is missing expected columns: {missing}")
    if len(df) != 4:
        print(f"WARNING: expected 4 experiment rows, found {len(df)}. Continuing anyway.")
    return df


def plot_accuracy_auc(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(df))
    width = 0.35
    ax.bar(x - width / 2, df["accuracy"], width, label="Accuracy")
    ax.bar(x + width / 2, df["auc"], width, label="AUC-ROC")
    ax.set_xticks(x)
    ax.set_xticklabels(df["experiment"] + "\n" + df["model"] + "\n" + df["feature_set"], fontsize=8)
    ax.set_ylim(0, 1)
    ax.set_ylabel("Score")
    ax.set_title("Accuracy and AUC-ROC across all 4 experiments")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, "accuracy_auc_comparison.png"), dpi=150)
    plt.close(fig)


def plot_f1_by_class(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(df))
    width = 0.35
    ax.bar(x - width / 2, df["f1_label1"], width, label="F1 (label=1, operating)")
    ax.bar(x + width / 2, df["f1_label0"], width, label="F1 (label=0, closed)")
    ax.set_xticks(x)
    ax.set_xticklabels(df["experiment"] + "\n" + df["model"] + "\n" + df["feature_set"], fontsize=8)
    ax.set_ylim(0, 1)
    ax.set_ylabel("F1 score")
    ax.set_title("F1 by class across all 4 experiments\n(class imbalance: ~13% closed / 87% operating - watch label=0's bar closely)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, "f1_by_class_comparison.png"), dpi=150)
    plt.close(fig)


def plot_confusion_matrices(df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(10, 9))
    for ax, (_, row) in zip(axes.flat, df.iterrows()):
        matrix = np.array([[row["tn"], row["fp"]], [row["fn"], row["tp"]]])
        sns.heatmap(
            matrix, annot=True, fmt=".0f", cmap="Blues", cbar=False, ax=ax,
            xticklabels=["Pred 0 (closed)", "Pred 1 (operating)"],
            yticklabels=["True 0 (closed)", "True 1 (operating)"],
        )
        ax.set_title(f"{row['experiment']}\n{row['model']} / {row['feature_set']}", fontsize=9)
    fig.suptitle("Confusion matrices - all 4 experiments")
    fig.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, "confusion_matrices.png"), dpi=150)
    plt.close(fig)


def plot_sentiment_lift(df: pd.DataFrame) -> None:
    """This project's actual research question: does adding sentiment
    features help? Isolates Experiment 1 (Financial-only LR) vs
    Experiment 3 (Financial+Sentiment LR), and Experiment 2 (Financial-
    only SVM) vs Experiment 4 (Financial+Sentiment SVM) - NOT just a
    ranking of all 4 by raw accuracy, which would conflate the
    model-choice effect (LR vs SVM) with the feature-choice effect
    (financial-only vs +sentiment)."""
    lr = df[df["model"] == "Logistic Regression"].sort_values("feature_set")
    svm = df[df["model"] == "Binary SVM"].sort_values("feature_set")

    fig, axes = plt.subplots(1, 2, figsize=(11, 5), sharey=True)
    for ax, sub, title in zip(axes, [lr, svm], ["Logistic Regression", "Binary SVM"]):
        metrics = ["accuracy", "auc", "f1_label0"]
        labels = ["Accuracy", "AUC-ROC", "F1 (label=0, closed)"]
        x = np.arange(len(metrics))
        width = 0.35
        financial_only = sub[sub["feature_set"] == "Financial-only"][metrics].values.flatten()
        financial_sentiment = sub[sub["feature_set"] == "Financial+Sentiment"][metrics].values.flatten()
        ax.bar(x - width / 2, financial_only, width, label="Financial-only")
        ax.bar(x + width / 2, financial_sentiment, width, label="Financial+Sentiment")
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylim(0, 1)
        ax.set_title(title)
        ax.legend()
    fig.suptitle("Does adding sentiment features help? (Experiment 1 vs 3, 2 vs 4)")
    fig.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, "sentiment_lift.png"), dpi=150)
    plt.close(fig)


def main() -> None:
    df = load_results()
    print(f"Loaded {len(df)} experiment results from {RESULTS_CSV}")
    print(df.to_string(index=False))

    plot_accuracy_auc(df)
    plot_f1_by_class(df)
    plot_confusion_matrices(df)
    plot_sentiment_lift(df)

    print(f"\nWrote 4 PNG charts to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
