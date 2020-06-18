from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import asyncio, queue, __main__

class ThreadTasks():
    def __init__(self):
        self.reset()
        self._executor = ThreadPoolExecutor()
                
    def queue(self):
        return queue.Queue()

    def reset(self):
        self._tasks = []
        return self

    def add(self, callback, **kwargs):
        self._tasks.append((callback, kwargs))
        return self
    
    @property
    def tasks(self):
        return self._tasks
    
    def count(self):
        return count(self.tasks)
    
    def __submit(self):
        tasks = [self._executor.submit(task[0], **task[1]) for task in self.tasks]
        self.reset()
        return tasks
    
    def executor(self):
        return [task.result() for task in as_completed(self.__submit())]
    
class ProcessTasks(ThreadTasks):
    def __init__(self):
        self.reset()
        self._executor = ProcessPoolExecutor()

if hasattr(__main__, '__file__'):
    class AsyncTasks(ThreadTasks):
        def __init__(self):
            self.reset()
                
        def queue(self):
            return asyncio.Queue()

        async def __callback(self, task):
            return task[0](**task[1])
        
        async def __submit(self):
            tasks = [self.__callback(task) for task in self._tasks]
            self.reset()
            return await asyncio.gather(*tasks)
                
        def executor(self):
            return asyncio.get_event_loop().run_until_complete(self.__submit())
else:
    class AsyncTasks(ThreadTasks):
        pass

if __name__ == '__main__':
    import time

    def test(i):
        return i
        
    tasks = AsyncTasks()
    tasks.add(test, i=1)
    tasks.add(test, i=2)
    tasks.add(test, i=3)

    s = time.perf_counter()
    result = tasks.executor()
    elapsed = time.perf_counter() - s
    print(f"Script executed in {elapsed:0.2f} seconds.")
    print(result)
    print(tasks.queue())