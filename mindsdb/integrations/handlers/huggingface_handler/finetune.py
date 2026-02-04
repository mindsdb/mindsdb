import evaluate
import nltk
import numpy as np
from datasets import Dataset
from transformers import (
    AutoConfig,
    AutoModelForSeq2SeqLM,
    AutoModelForSequenceClassification,
    AutoTokenizer,
    DataCollatorForSeq2Seq,
    Seq2SeqTrainingArguments,
    Trainer,
    TrainingArguments,
)

# todo add support for question answering task
# todo add support for fill mask
# todo add support for text_generation (causal language model)
# todo add support for text_2_text generation


def _finetune_cls(df, args):
    df = df.rename(columns={args["target"]: "labels", args["input_column"]: "text"})
    tokenizer_from = args.get("using", {}).get("tokenizer_from", args["model_name"])
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_from)
    dataset = Dataset.from_pandas(df)

    def _tokenize_text_cls_fn(examples):
        return tokenizer(examples["text"], padding="max_length", truncation=True)

    tokenized_datasets = dataset.map(_tokenize_text_cls_fn, batched=True)
    ds = tokenized_datasets.shuffle(seed=42).train_test_split(test_size=args.get("eval_size", 0.1))
    train_ds = ds["train"]
    eval_ds = ds["test"]

    ft_args = args.get("using", {}).get("trainer_args", {})
    ft_args["output_dir"] = args["model_folder"]

    n_labels = len(args["labels_map"])
    # todo replace for prod
    assert n_labels == df["labels"].nunique(), (
        f"Label mismatch! Ensure labels match what the model was originally trained on. Found {df['labels'].nunique()} classes, expected {n_labels}."
    )  # noqa
    # TODO: ideally check that labels are a subset of the original ones, too.
    config = AutoConfig.from_pretrained(args["model_name"])
    model = AutoModelForSequenceClassification.from_pretrained(args["model_name"], config=config)
    metric = evaluate.load("accuracy")
    training_args = TrainingArguments(**ft_args)

    def _compute_metrics(eval_pred):
        logits, labels = eval_pred
        predictions = np.argmax(logits, axis=-1)
        return metric.compute(predictions=predictions, references=labels)

    # generate trainer and finetune
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=eval_ds,
        compute_metrics=_compute_metrics,
    )

    return tokenizer, trainer


# TODO: merge with summarization?
def _finetune_translate(df, args):
    config = AutoConfig.from_pretrained(args["model_name"])
    df = df.rename(columns={args["target"]: "translation", args["input_column"]: "text"})
    tokenizer_from = args.get("using", {}).get("tokenizer_from", args["model_name"])
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_from)
    dataset = Dataset.from_pandas(df)

    def _tokenize_translate_fn(examples):
        source_lang = args["lang_input"]
        target_lang = args["lang_output"]
        max_target_length = config.task_specific_params["summarization"]["max_length"]
        prefix = f"translate {source_lang} to {target_lang}: "
        inputs = [prefix + ex for ex in examples["text"]]
        targets = [ex for ex in examples["translation"]]
        model_inputs = tokenizer(inputs, max_length=config.n_positions, truncation=True)

        # Setup the tokenizer for targets
        with tokenizer.as_target_tokenizer():
            labels = tokenizer(targets, max_length=max_target_length, truncation=True)

        model_inputs["labels"] = labels["input_ids"]
        return model_inputs

    tokenized_datasets = dataset.map(_tokenize_translate_fn, batched=True)
    ds = tokenized_datasets.shuffle(seed=42).train_test_split(test_size=args.get("eval_size", 0.1))
    train_ds = ds["train"]
    eval_ds = ds["test"]
    ft_args = args.get("using", {}).get("trainer_args", {})
    ft_args["output_dir"] = args["model_folder"]
    ft_args["predict_with_generate"] = True

    model = AutoModelForSeq2SeqLM.from_pretrained(args["model_name"], config=config)
    model.resize_token_embeddings(len(tokenizer))
    training_args = Seq2SeqTrainingArguments(**ft_args)
    data_collator = DataCollatorForSeq2Seq(tokenizer, model=model)

    # generate trainer and finetune
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=eval_ds,
        data_collator=data_collator,
        # compute_metrics=_compute_metrics,
    )

    return tokenizer, trainer


def _finetune_summarization(df, args):
    df = df.rename(columns={args["target"]: "summary", args["input_column"]: "text"})
    tokenizer_from = args.get("using", {}).get("tokenizer_from", args["model_name"])
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_from)
    dataset = Dataset.from_pandas(df)
    config = AutoConfig.from_pretrained(args["model_name"])

    def _tokenize_summarize_fn(examples):
        prefix = "summarize: " if "t5" in args["model_name"] else ""
        inputs = [prefix + doc for doc in examples["text"]]
        model_inputs = tokenizer(
            inputs,
            padding="max_length",
            truncation=True,
            max_length=config.max_position_embeddings,
            pad_to_max_length=True,
        )  # noqa
        labels = tokenizer(
            text_target=examples["summary"],
            max_length=config.max_position_embeddings,
            truncation=True,
        )  # noqa
        model_inputs["labels"] = labels["input_ids"]
        return model_inputs

    tokenized_datasets = dataset.map(_tokenize_summarize_fn, batched=True)
    ds = tokenized_datasets.shuffle(seed=42).train_test_split(test_size=args.get("eval_size", 0.1))
    train_ds = ds["train"]
    eval_ds = ds["test"]

    ft_args = args.get("using", {}).get("trainer_args", {})
    ft_args["output_dir"] = args["model_folder"]
    ft_args["predict_with_generate"] = True

    model = AutoModelForSeq2SeqLM.from_pretrained(args["model_name"], config=config)
    metric = evaluate.load("rouge")
    training_args = Seq2SeqTrainingArguments(**ft_args)
    data_collator = DataCollatorForSeq2Seq(tokenizer, model=model)

    def _compute_metrics(eval_pred):
        # ref: github.com/huggingface/notebooks/blob/main/examples/summarization.ipynb
        predictions, labels = eval_pred
        decoded_preds = tokenizer.batch_decode(predictions, skip_special_tokens=True)
        decoded_labels = tokenizer.batch_decode(labels, skip_special_tokens=True)

        # Rogue expects a newline after each sentence
        decoded_preds = ["\n".join(nltk.sent_tokenize(pred.strip())) for pred in decoded_preds]
        decoded_labels = ["\n".join(nltk.sent_tokenize(label.strip())) for label in decoded_labels]

        result = metric.compute(
            predictions=decoded_preds,
            references=decoded_labels,
            use_stemmer=True,
            use_aggregator=True,
        )
        result = {key: value * 100 for key, value in result.items()}
        prediction_lens = [np.count_nonzero(pred != tokenizer.pad_token_id) for pred in predictions]
        result["gen_len"] = np.mean(prediction_lens)  # todo: remove?
        return {k: round(v, 4) for k, v in result.items()}

    # generate trainer and finetune
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=eval_ds,
        data_collator=data_collator,
        compute_metrics=_compute_metrics,
    )

    return tokenizer, trainer


def _finetune_fill_mask(df, args):
    raise NotImplementedError("Finetuning fill-mask models is not yet supported.")


def _finetune_text_generation(df, args):
    raise NotImplementedError("Finetuning text-generation models is not yet supported.")


def _finetune_question_answering(df, args):
    raise NotImplementedError("Finetuning question-answering models is not yet supported.")


def _finetune_text_2_text_generation(df, args):
    raise NotImplementedError("Finetuning text-2-text generation models is not yet supported.")
