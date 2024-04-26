---
title: "Datasets and data-aware scheduling in Airflow"
sidebar_label: "Datasets and data-aware scheduling"
description: "Using datasets to implement DAG dependencies and scheduling in Airflow."
id: airflow-datasets
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';
import dataset_producer from '!!raw-loader!../code-samples/dags/airflow-datasets/dataset_producer.py';
import dataset_producer_traditional from '!!raw-loader!../code-samples/dags/airflow-datasets/dataset_producer_traditional.py';
import dataset_consumer from '!!raw-loader!../code-samples/dags/airflow-datasets/dataset_consumer.py';
import dataset_consumer_traditional from '!!raw-loader!../code-samples/dags/airflow-datasets/dataset_consumer_traditional.py';
import example_sdk_datasets from '!!raw-loader!../code-samples/dags/airflow-datasets/example_sdk_datasets.py';

With Datasets, DAGs that access the same data can have explicit, visible relationships, and DAGs can be scheduled based on updates to these datasets. This feature helps make Airflow data-aware and expands Airflow scheduling capabilities beyond time-based methods such as cron.

Datasets can help resolve common issues. For example, consider a data engineering team with a DAG that creates a dataset and an analytics team with a DAG that analyses the dataset. Using datasets, the data analytics DAG runs only when the data engineering team's DAG publishes the dataset.

In this guide, you'll learn about datasets in Airflow and how to use them to implement triggering of DAGs based on dataset updates. You'll also learn how datasets work with the Astro Python SDK.

:::info

Datasets are a separate feature from object storage, which allows you to interact with files in cloud and local object storage systems. To learn more about using Airflow to interact with files, see [Use Airflow object storage to interact with cloud storage in an ML pipeline](airflow-object-storage-tutorial.md).

:::

:::tip Other ways to learn

There are multiple resources for learning about this topic. See also:

- Astronomer Academy: [Airflow: Datasets](https://academy.astronomer.io/astro-runtime-datasets) module.
- Webinar: [Data Driven Scheduling](https://www.astronomer.io/events/webinars/data-driven-scheduling/).
- Use case: [Orchestrate machine learning pipelines with Airflow datasets](use-case-airflow-datasets-multi-team-ml.md).

:::

## Assumed knowledge

To get the most out of this guide, you should have an existing knowledge of:

- Airflow scheduling concepts. See [Schedule DAGs in Airflow](scheduling-in-airflow.md).
- Creating dependencies between DAGs. See [Cross-DAG Dependencies](cross-dag-dependencies.md).
- The Astro Python SDK. See [Using the Astro Python SDK](https://docs.astronomer.io/tutorials/astro-python-sdk).

## Why use datasets?

Datasets allow you to define explicit dependencies between DAGs and updates to your data. This helps you to:

- Standardize communication between teams. Datasets can function like an API to communicate when data in a specific location has been updated and is ready for use.
- Reduce the amount of code necessary to implement [cross-DAG dependencies](cross-dag-dependencies.md). Even if your DAGs don't depend on data updates, you can create a dependency that triggers a DAG after a task in another DAG updates a dataset.
- Get better visibility into how your DAGs are connected and how they depend on data. The **Datasets** tab in the Airflow UI shows a graph of all dependencies between DAGs and datasets in your Airflow environment.
- Reduce costs, because datasets do not use a worker slot in contrast to sensors or other implementations of cross-DAG dependencies.

:::note Listening for dataset changes

As of Airflow 2.8, you can use [listeners](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/listeners.html#listeners) to enable Airflow to notify you when certain dataset events occur. There are two listener hooks for the following events: 

- on_dataset_created
- on_dataset_changed

For examples, refer to our [Create Airflow listeners tutorial](https://docs.astronomer.io/learn/airflow-listeners).
:::

## Dataset concepts

You can define datasets in your Airflow environment and use them to create dependencies between DAGs. To define a dataset, instantiate the `Dataset` class and provide a string to identify the location of the dataset. This string must be in the form of a valid Uniform Resource Identifier (URI). See [What is valid URI?](http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html#what-is-valid-uri) for detailed information.

Currently, the URI is not used to connect to an external system and there is no awareness of the content or location of the dataset. However, using this naming convention helps you to easily identify the datasets that your DAG accesses and ensures compatibility with future Airflow features.

The dataset URI is saved as plain text, so it is recommended that you hide sensitive values using environment variables or a secrets backend.

You can reference the dataset in a task by passing it to the task's `outlets` parameter. `outlets` is part of the `BaseOperator`, so it's available to every Airflow operator. 

When you define a task's `outlets` parameter, Airflow labels the task as a producer task that updates the datasets. It is up to you to determine which tasks should be considered producer tasks for a dataset. As long as a task has an outlet dataset, Airflow considers it a producer task even if that task doesn't operate on the referenced dataset. In the following example, the `write_instructions_to_file` and `write_info_to_file` are both producer tasks because they have defined outlets.

<Tabs
    defaultValue="taskflow"
    groupId="dataset-concepts"
    values={[
        {label: 'TaskFlow API', value: 'taskflow'},
        {label: 'Traditional syntax', value: 'traditional'},
    ]}>
<TabItem value="taskflow">

<CodeBlock language="python">{dataset_producer}</CodeBlock>

</TabItem>

<TabItem value="traditional">

<CodeBlock language="python">{dataset_producer_traditional}</CodeBlock>

</TabItem>

</Tabs>

A consumer DAG runs whenever the dataset(s) it is scheduled on is updated by a producer task, rather than running on a time-based schedule. For example, if you have a DAG that should run when the `INSTRUCTIONS` and `INFO` datasets are updated, you define the DAG's schedule using the names of those two datasets.

Any DAG that is scheduled with a dataset is considered a consumer DAG even if that DAG doesn't actually access the referenced dataset. In other words, it's up to you as the DAG author to correctly reference and use datasets.

<Tabs
    defaultValue="taskflow"
    groupId="dataset-concepts"
    values={[
        {label: 'TaskFlow API', value: 'taskflow'},
        {label: 'Traditional syntax', value: 'traditional'},
    ]}>
<TabItem value="taskflow">

<CodeBlock language="python">{dataset_consumer}</CodeBlock>

</TabItem>

<TabItem value="traditional">

<CodeBlock language="python">{dataset_consumer_traditional}</CodeBlock>

</TabItem>

</Tabs>

Any number of datasets can be provided to the `schedule` parameter as a list or as an expression using [conditional logic](#conditional-dataset-scheduling). If the Datasets are provided in a list, the DAG is triggered after all of the datasets have received at least one update due to a producing task completing successfully. 

When you work with datasets, keep the following considerations in mind:

- Datasets can only be used by DAGs in the same Airflow environment.
- Airflow monitors datasets only within the context of DAGs and tasks. It does not monitor updates to datasets that occur outside of Airflow.
- Consumer DAGs that are scheduled on a dataset are triggered every time a task that updates that dataset completes successfully. For example, if `task1` and `task2` both produce `dataset_a`, a consumer DAG of `dataset_a` runs twice - first when `task1` completes, and again when `task2` completes.
- Consumer DAGs scheduled on a dataset are triggered as soon as the first task with that dataset as an outlet finishes, even if there are downstream producer tasks that also operate on the dataset.

Airflow 2.9 added several new features to datasets:
- [Conditional Dataset Scheduling](#conditional-dataset-scheduling)
- [Combined Dataset and Time-based Scheduling](#combined-dataset-and-time-based-scheduling)
- Datasets are now shown in the **Graph** view of a DAG in the Airflow UI. The `upstream1` DAG in the screenshot below is a consumer of the `dataset0` dataset, and has one task `update_dataset_1` that updates the `dataset1` dataset.

    ![Screenshot of the Airflow UI showing the graph view of a DAG with a ](/img/guides/airflow-datasets_in_graph_view.png)

For more information about datasets, see [Data-aware scheduling](https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html). 

The **Datasets** tab, and the **DAG Dependencies** view in the Airflow UI give you observability for datasets and data dependencies in the DAG's schedule.

On the **DAGs** view, you can see that your `dataset_downstream_1_2` DAG is scheduled on two producer datasets (one in `dataset_upstream1` and `dataset_upstream2`). When Datasets are provided as a list, the DAG is scheduled to run after all Datasets in the list have received at least one update. In the following screenshot, the `dataset_downstream_1_2` DAG's next run is pending one dataset update. At this point the `dataset_upstream` DAG has run and updated its dataset, but the `dataset_upstream2` DAG has not.

![DAGs View](/img/guides/dags_view_dataset_schedule.png)

The **Datasets** tab shows a list of all datasets in your Airflow environment and a graph showing how your DAGs and datasets are connected. You can filter the lists of Datasets by recent updates.

![Datasets View](/img/guides/datasets_view_overview.png)

Click one of the datasets to display a list of task instances that updated the dataset and a highlighted view of that dataset and its connections on the graph.

![Datasets Highlight](/img/guides/datasets_view_highlight.png)

The **DAG Dependencies** view (found under the **Browse** tab) shows a graph of all dependencies between DAGs (in green) and datasets (in orange) in your Airflow environment.

![DAG Dependencies View](/img/guides/dag_dependencies.png)

:::note

DAGs that are triggered by datasets do not have the concept of a data interval. If you need information about the triggering event in your downstream DAG, you can use the parameter `triggering_dataset_events` from the context. This parameter provides a list of all the triggering dataset events with parameters `[timestamp, source_dag_id, source_task_id, source_run_id, source_map_index ]`.

:::

### Updating a dataset

As of Airflow 2.9+ there are three ways to update a dataset:

- A task with an outlet parameter that references the dataset completes successfully.
- A `POST` request to the [datasets endpoint of the Airflow REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#tag/Dataset).
- A manual update in the Airflow UI. 

    ![Screenshot of the Airflow UI showing the view history of updates to a specific dataset in the datasets tab with the play button to update the dataset manually highlighted](/img/guides/airflow-datasets_manually_update_dataset.png)

### Conditional dataset scheduling

In Airflow 2.9 and later, you can use logical operators to combine any number of datasets provided to the `schedule` parameter. The logical operators supported are `|` for OR and `&` for AND. 

For example, to schedule a DAG on an update to either `dataset1`, `dataset2`, `dataset3`, or `dataset4`, you can use the following syntax. Note that the full statement is wrapped in `()`.

<Tabs
    defaultValue="taskflow"
    groupId="conditional-dataset-scheduling"
    values={[
        {label: 'TaskFlow API', value: 'taskflow'},
        {label: 'Traditional syntax', value: 'traditional'},
    ]}>
<TabItem value="taskflow">

```python
from airflow.decorators import dag
from airflow.models.datasets import Dataset
from pendulum import datetime

@dag(
    start_date=datetime(2024, 3, 1),
    schedule=(
        Dataset("dataset1")
        | Dataset("dataset2")
        | Dataset("dataset3")
        | Dataset("dataset4")
    ),  # Use () instead of [] to be able to use conditional dataset scheduling!
    catchup=False,
)
def downstream1_on_any():

    # your tasks here

downstream1_on_any()
```

</TabItem>
<TabItem value="traditional">

```python
from airflow.models import DAG
from airflow.models.datasets import Dataset
from pendulum import datetime

with DAG(
    dag_id="downstream1_on_any",
    start_date=datetime(2024, 3, 1),
    schedule=(
        Dataset("dataset1")
        | Dataset("dataset2")
        | Dataset("dataset3")
        | Dataset("dataset4")
    ),  # Use () instead of [] to be able to use conditional dataset scheduling!
    catchup=False,
):

    # your tasks here
```

</TabItem>
</Tabs>

The `downstream1_on_any` DAG is triggered whenever any of the datasets `dataset1`, `dataset2`, `dataset3`, or `dataset4` are updated. When clicking on **x of 4 Datasets  updated** in the DAGs view, you can see the dataset expression that defines the schedule.

![Screenshot of the Airflow UI with a pop up showing the dataset expression for the downstream1_on_any DAG listing the 4 datasets under "any"](/img/guides/airflow-datasets_dataset_expression_any.png)

You can also combine the logical operators to create more complex expressions. For example, to schedule a DAG on an update to either `dataset1` or `dataset2` and either `dataset3` or `dataset4`, you can use the following syntax:

<Tabs
    defaultValue="taskflow"
    groupId="conditional-dataset-scheduling"
    values={[
        {label: 'TaskFlow API', value: 'taskflow'},
        {label: 'Traditional syntax', value: 'traditional'},
    ]}>
<TabItem value="taskflow">

```python
from airflow.decorators import dag
from airflow.models.datasets import Dataset
from pendulum import datetime

@dag(
    start_date=datetime(2024, 3, 1),
    schedule=(
        (Dataset("dataset1") | Dataset("dataset2"))
        & (Dataset("dataset3") | Dataset("dataset4"))
    ),  # Use () instead of [] to be able to use conditional dataset scheduling!
    catchup=False
)
def downstream2_one_in_each_group():

    # your tasks here

downstream2_one_in_each_group()
```

</TabItem>
<TabItem value="traditional">

```python
from airflow.models import DAG
from airflow.models.datasets import Dataset
from pendulum import datetime

with DAG(
    dag_id="downstream2_one_in_each_group",
    start_date=datetime(2024, 3, 1),
    schedule=(
        (Dataset("dataset1") | Dataset("dataset2"))
        & (Dataset("dataset3") | Dataset("dataset4"))
    ),  # Use () instead of [] to be able to use conditional dataset scheduling!
    catchup=False,
):

    # your tasks here
```

</TabItem>
</Tabs>

The dataset expression this schedule creates is:

```text
{
  "all": [
    {
      "any": [
        "dataset1",
        "dataset2"
      ]
    },
    {
      "any": [
        "dataset3",
        "dataset4"
      ]
    }
  ]
}
```

### Combined dataset and time-based scheduling

In Airflow 2.9 and later, you can combine dataset-based scheduling with time-based scheduling with the `DatasetOrTimeSchedule` timetable. A DAG scheduled with this timetable will run either when its `timetable` condition is met or when its `dataset` condition is met.

The DAG shown below runs on a time-based schedule defined by the `0 0 * * *` cron expression, which is every day at midnight. The DAG also runs when either `dataset3` or `dataset4` is updated.

<Tabs
    defaultValue="taskflow"
    groupId="combined-dataset-and-time-based-scheduling"
    values={[
        {label: 'TaskFlow API', value: 'taskflow'},
        {label: 'Traditional syntax', value: 'traditional'},
    ]}>
<TabItem value="taskflow">

```python
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable


@dag(
    start_date=datetime(2024, 3, 1),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        datasets=(Dataset("dataset3") | Dataset("dataset4")),
        # Use () instead of [] to be able to use conditional dataset scheduling!
    ), 
    catchup=False,
)
def toy_downstream3_dataset_and_time_schedule():

    # your tasks here

toy_downstream3_dataset_and_time_schedule()
```

</TabItem>
<TabItem value="traditional">

```python
from airflow.models import DAG
from airflow.datasets import Dataset
from pendulum import datetime
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable


with DAG(
    dag_id="toy_downstream3_dataset_and_time_schedule",
    start_date=datetime(2024, 3, 1),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        datasets=(Dataset("dataset3") | Dataset("dataset4")),
        # Use () instead of [] to be able to use conditional dataset scheduling!
    ), 
    catchup=False,
):
    # your tasks here

```

</TabItem>
</Tabs>

## Datasets with the Astro Python SDK

If you are using the [Astro Python SDK](https://docs.astronomer.io/tutorials/astro-python-sdk) version 1.1 or later, you do not need to make any code updates to use datasets. Datasets are automatically registered for any functions with output tables and you do not need to define any `outlet` parameters. 

The following example DAG results in three registered datasets: one for each `load_file` function and one for the resulting data from the `transform` function.

<CodeBlock language="python">{example_sdk_datasets}</CodeBlock>

![SDK datasets](/img/guides/sdk_datasets.png)