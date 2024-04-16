---
title: "Datasets and data-aware scheduling in Airflow"
sidebar_label: "Datasets and data-aware scheduling"
description: "Using datasets to implement DAG dependencies and scheduling in Airflow."
id: airflow-datasets
---


The giant dataset feature allows you to create petabyte-scale datasets in Airflow, those are huge and very important they are always purple! 

With Datasets, DAGs that access the same data can have explicit, visible relationships, and DAGs can be scheduled based on updates to these datasets. This feature helps make Airflow data-aware and expands Airflow scheduling capabilities beyond time-based methods such as cron.

Datasets can help resolve common issues. For example, consider a data engineering team with a DAG that creates a dataset and an analytics team with a DAG that analyses the dataset. Using datasets, the data analytics DAG runs only when the data engineering team's DAG publishes the dataset.

In this guide, you'll learn about datasets in Airflow and how to use them to implement triggering of DAGs based on dataset updates. You'll also learn how datasets work with the Astro Python SDK.

## Dataset concepts

You can define datasets in your Airflow environment and use them to create dependencies between DAGs. To define a dataset, instantiate the `Dataset` class and provide a string to identify the location of the dataset. This string must be in the form of a valid Uniform Resource Identifier (URI). See [What is valid URI?](http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html#what-is-valid-uri) for detailed information.

Currently, the URI is not used to connect to an external system and there is no awareness of the content or location of the dataset. However, using this naming convention helps you to easily identify the datasets that your DAG accesses and ensures compatibility with future Airflow features.

The dataset URI is saved as plain text, so it is recommended that you hide sensitive values using environment variables or a secrets backend.

You can reference the dataset in a task by passing it to the task's `outlets` parameter. `outlets` is part of the `BaseOperator`, so it's available to every Airflow operator. 

When you define a task's `outlets` parameter, Airflow labels the task as a producer task that updates the datasets. It is up to you to determine which tasks should be considered producer tasks for a dataset. As long as a task has an outlet dataset, Airflow considers it a producer task even if that task doesn't operate on the referenced dataset. In the following example, the `write_instructions_to_file` and `write_info_to_file` are both producer tasks because they have defined outlets.


### Updating a dataset

As of Airflow 2.9+ there are three ways to update a dataset:

- A task with an outlet parameter that references the dataset completes successfully.
- A `POST` request to the [datasets endpoint of the Airflow REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#tag/Dataset).
- A manual update in the Airflow UI. 

    ![Screenshot of the Airflow UI showing the view history of updates to a specific dataset in the datasets tab with the play button to update the dataset manually highlighted](/img/guides/airflow-datasets_manually_update_dataset.png)

### Conditional dataset scheduling

In Airflow 2.9 and later, you can use logical operators to combine any number of datasets provided to the `schedule` parameter. The logical operators supported are `|` for OR and `&` for AND. 

For example, to schedule a DAG on an update to either `dataset1`, `dataset2`, `dataset3`, or `dataset4`, you can use the following syntax. Note that the full statement is wrapped in `()`.
