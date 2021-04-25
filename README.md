# Hacker News Pipeline
### Processing news using a task-based data pipeline to filter, clean, aggregate, and summarize data.
This investigation looks at a dataset of news/interest stories from 2014. Using this dataset, a sequence of basic natural language processing tasks are run using an implementation of a pipeline. The goal is to find the top 100 keywords of Hacker News posts in 2014. Because Hacker News is the most popular technology social media site, this will give us an understanding of the most talked about tech topics in 2014.

The investigation demonstrates the use of a task-based data pipeline, which orders tasks using a directed acyclic graph. The pipeline is used to **filter, clean, aggregate, and summarize data** in a sequence of tasks that will apply these transformations. Some tasks are written as 'generator' functions that successive tasks can use to iterate through values, instead of passing through all data at once.

The DAG class and Pipeline class have been implementated in [task_pipeline.py](https://github.com/darrenpaine/HackerNews-Pipeline/blob/main/task_pipeline.py)
