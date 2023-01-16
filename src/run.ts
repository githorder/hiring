import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(
  executor: IExecutor,
  queue: AsyncIterable<ITask>,
  maxThreads = 0
) {
  maxThreads = Math.max(0, maxThreads);

  let tasksInExecution: Promise<void>[] = [];
  let tasksInQueue = [];
  let taskIds: number[] = [];

  if (maxThreads === 0) {
    for await (const task of queue) {
      if (taskIds[task.targetId] === undefined) {
        tasksInExecution[task.targetId] = executor.executeTask(task);
        taskIds[task.targetId] = task.targetId;
      } else {
        tasksInQueue.push(task);
      }
    }

    await Promise.all(tasksInExecution);
    tasksInExecution = [];
    tasksInQueue.sort((task1, task2) => task1.targetId - task2.targetId);

    for (let i = 0; i < 4; i++) {
      for (let j = i; j < tasksInQueue.length; j += 4) {
        tasksInExecution.push(executor.executeTask(tasksInQueue[j]));
      }

      await Promise.all(tasksInExecution);
      tasksInExecution = [];
    }
  } else {
    let iterator = queue[Symbol.asyncIterator]();
    let iteration;

    do {
      for await (const task of queue) {
        if (taskIds.indexOf(task.targetId) === -1) {
          if (tasksInExecution.length === maxThreads) {
            await Promise.all(tasksInExecution);
            tasksInExecution = [];
          }

          tasksInExecution.push(executor.executeTask(task));
          taskIds.push(task.targetId);
        } else {
          tasksInQueue.push(task);
        }
      }

      tasksInQueue.sort((task1, task2) => task1.targetId - task2.targetId);
      const optimizedTasksQueue = [];
      let subQueueLength: number = 0;

      for (let i = 0; i < 4; i++) {
        const subQueue = [];
        for (let j = i; j < tasksInQueue.length; j += 4) {
          subQueue.push(tasksInQueue[j]);
        }

        subQueueLength = subQueue.length;
        optimizedTasksQueue.push(subQueue);
      }

      for (const curSubQueue of optimizedTasksQueue) {
        if (subQueueLength < maxThreads) {
          await Promise.all(tasksInExecution);
          tasksInExecution = [];
          for (const curTask of curSubQueue) {
            tasksInExecution.push(executor.executeTask(curTask));
          }
          await Promise.all(tasksInExecution);
          tasksInExecution = [];
        } else if (subQueueLength >= maxThreads) {
          for (const curTask of curSubQueue) {
            if (tasksInExecution.length === maxThreads) {
              await Promise.all(tasksInExecution);
              tasksInExecution = [];
            }
            tasksInExecution.push(executor.executeTask(curTask));
          }
        }
      }

      await Promise.all(tasksInExecution);
      tasksInExecution = [];
      tasksInQueue = [];
      iteration = await iterator.next();

      if (!iteration.done) {
        tasksInExecution.push(executor.executeTask(iteration.value));
        taskIds.push(iteration.value.targetId);
      }
    } while (!iteration.done);
  }
}
