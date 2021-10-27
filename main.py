from Pipelines import Pipeline

from Tasks.Creator import TaskPool
from Tasks.Executor import TaskExecutor


def main():
    """
    Main driver of the program
    """
    p = Pipeline()
    p.execute_pipelines()
    # tp = TaskPool()
    # tp.create_task_pool()
    # te = TaskExecutor()
    # te.set_task(tp.pool['task_2'])
    # te.execute_task()


if __name__ == "__main__":
    main()
    print("sajal")
