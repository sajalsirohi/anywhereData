from Tasks.Executor import TaskExecutor
from Tasks.Creator import TaskPool


def main():
    """
    Main driver of the program
    """
    te = TaskExecutor()
    tp = TaskPool()
    tp.create_task_pool()
    te.set_task(tp.pool['task_1'])
    te.execute_task()


if __name__ == "__main__":
    main()
    print("sajal")
