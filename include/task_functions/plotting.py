from matplotlib import pyplot as plt
import pandas as pd


def plot_model_train_val_graph(
    fine_tuned_model: str, df: pd.DataFrame, ts: str
) -> None:
    """
    Plot the training and validation loss and accuracy over time.
    Args:
        fine_tuned_model (str): The name of the fine-tuned model.
        df (pd.DataFrame): The DataFrame containing the training results.
        ts (str): The timestamp of the fine-tuning run.
    """

    # TODO: prettify
    plt.figure(figsize=(12, 10))

    plt.suptitle(f"Fine-tuning model_results: {fine_tuned_model}", fontsize=16)

    plt.subplot(2, 2, 1)
    plt.plot(df["step"], df["train_loss"], label="Training Loss", color="blue")
    plt.xlabel("Training Step")
    plt.ylabel("Loss")
    plt.title("Training Loss Over Time")
    plt.legend()

    plt.subplot(2, 2, 2)
    plt.plot(
        df["step"],
        df["train_accuracy"],
        label="Training Accuracy",
        color="green",
    )
    plt.xlabel("Training Step")
    plt.ylabel("Accuracy")
    plt.title("Training Accuracy Over Time")
    plt.legend()

    plt.subplot(2, 2, 3)
    plt.plot(df["step"], df["valid_loss"], label="Validation Loss", color="red")
    plt.xlabel("Training Step")
    plt.ylabel("Loss")
    plt.title("Validation Loss Over Time")
    plt.legend()

    plt.subplot(2, 2, 4)
    plt.plot(
        df["step"],
        df["valid_mean_token_accuracy"],
        label="Validation Mean Token Accuracy",
        color="purple",
    )
    plt.xlabel("Training Step")
    plt.ylabel("Accuracy")
    plt.title("Validation Mean Token Accuracy Over Time")
    plt.legend()

    plt.tight_layout()
    plt.savefig(f"include/model_results/plots/{ts}_fine_tuning_model_results.png")
