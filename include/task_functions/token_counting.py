import tiktoken
import numpy as np


def _get_encoding(encoding: str = "cl100k_base") -> tiktoken.Encoding:
    return tiktoken.get_encoding(encoding)


# not exact!
# simplified from https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
def _num_tokens_from_messages(
    messages, tokens_per_message=3, tokens_per_name=1, encoding="cl100k_base"
):
    encoding = _get_encoding(encoding)
    num_tokens = 0
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 3
    return num_tokens


def _num_assistant_tokens_from_messages(messages, encoding="cl100k_base"):
    encoding = _get_encoding(encoding)
    num_tokens = 0
    for message in messages:
        if message["role"] == "assistant":
            num_tokens += len(encoding.encode(message["content"]))
    return num_tokens


def _print_distribution(values, name):
    print(f"\n#### Distribution of {name}:")
    print(f"min / max: {min(values)}, {max(values)}")
    print(f"mean / median: {np.mean(values)}, {np.median(values)}")
    print(f"p5 / p95: {np.quantile(values, 0.1)}, {np.quantile(values, 0.9)}")


def dataset_token_analysis(dataset, encoding):
    n_missing_system = 0
    n_missing_user = 0
    n_messages = []
    convo_lens = []
    assistant_message_lens = []

    for ex in dataset:
        messages = ex["messages"]
        if not any(message["role"] == "system" for message in messages):
            n_missing_system += 1
        if not any(message["role"] == "user" for message in messages):
            n_missing_user += 1
        n_messages.append(len(messages))
        convo_lens.append(_num_tokens_from_messages(messages, encoding=encoding))
        assistant_message_lens.append(
            _num_assistant_tokens_from_messages(messages, encoding=encoding)
        )

    print("Num examples missing system message:", n_missing_system)
    print("Num examples missing user message:", n_missing_user)
    _print_distribution(n_messages, "num_messages_per_example")
    _print_distribution(convo_lens, "num_total_tokens_per_example")
    _print_distribution(assistant_message_lens, "num_assistant_tokens_per_example")
    n_too_long = sum(l > 4096 for l in convo_lens)
    print(
        f"\n{n_too_long} examples may be over the 4096 token limit, they will be truncated during fine-tuning"
    )

    MAX_TOKENS_PER_EXAMPLE = 4096
    TARGET_EPOCHS = 10
    MIN_TARGET_EXAMPLES = 100
    MAX_TARGET_EXAMPLES = 25000
    MIN_DEFAULT_EPOCHS = 1
    MAX_DEFAULT_EPOCHS = 25

    n_epochs = TARGET_EPOCHS
    n_train_examples = len(dataset)
    if n_train_examples * TARGET_EPOCHS < MIN_TARGET_EXAMPLES:
        n_epochs = min(MAX_DEFAULT_EPOCHS, MIN_TARGET_EXAMPLES // n_train_examples)
    elif n_train_examples * TARGET_EPOCHS > MAX_TARGET_EXAMPLES:
        n_epochs = max(MIN_DEFAULT_EPOCHS, MAX_TARGET_EXAMPLES // n_train_examples)

    n_billing_tokens_in_dataset = sum(
        min(MAX_TOKENS_PER_EXAMPLE, length) for length in convo_lens
    )

    fine_tune_per_m_price = 8
    total_cost = fine_tune_per_m_price * n_billing_tokens_in_dataset / 1e6

    print(
        f"Dataset has ~{n_billing_tokens_in_dataset} tokens that will be charged for during training"
    )
    print(f"By default, you'll train for {n_epochs} epochs on this dataset")
    print(
        f"By default, you'll be charged for ~{n_epochs * n_billing_tokens_in_dataset} tokens"
    )
    print(
        f"At ${fine_tune_per_m_price} per 1M tokens for fine tuning, this dataset will cost approximately ${total_cost} to fine-tune"
    )

    return n_billing_tokens_in_dataset
