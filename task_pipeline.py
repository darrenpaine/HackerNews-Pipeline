# DAG = directed acyclic graph
from collections import deque
class DAG():
    """DAG = directed acyclic graph
    This is a simple implementation of the DAG to be used in the Pipeline implementation"""
    def __init__(self):
        self.graph = {}
    
    def add(self, node, to=None):
        if not self.graph.get(node, None):
            self.graph[node] = []
        if to:
            if not self.graph.get(to, None):
                self.graph[to] = []
            self.graph[node].append(to)
        # Add validity check.
        if len(self.sort()) > len(self.graph):
            raise Exception('Cycle detected')

    def in_degrees(self):
        self.degrees = {}
        for key, value in self.graph.items():
            if key not in self.degrees:
                self.degrees[key] = 0
            for to_node in value:
                self.degrees[to_node] = self.degrees.get(to_node, 0) + 1
                
    def sort(self):
        self.in_degrees()
        to_visit = deque()
        for node in self.graph:
            if self.degrees[node] == 0:
                to_visit.append(node)
        
        searched = []
        while to_visit:
            node = to_visit.popleft()
            for pointer in self.graph[node]:
                self.degrees[pointer] -= 1
                if self.degrees[pointer] == 0:
                    to_visit.append(pointer)
            searched.append(node)
        return searched

# Implementation of a Pipeline.
class Pipeline():
    """The Pipline allows tasks to be added from 'decorator' functions
    These tasks are placed onto the DAG, set up using a closure.
    The run() function executes the closure functions, passing
    the results on to the next function(s) in the graph."""
    def __init__(self):
        # Set up our internal list of tasks as a directed acyclic graph:
        self.tasks = DAG()
        
    def task(self, depends_on=None):
        # Add a closure function for the task to be run later:
        def inner(f):
            self.tasks.add(f)
            if depends_on:
                self.tasks.add(depends_on, f)
            return f
        return inner
    
    def run(self):
        # First sort our list of tasks:
        scheduled = self.tasks.sort()
        completed = {}
        
        # Go through the graph, running the tasks:
        for task in scheduled:
            for node, values in self.tasks.graph.items():
                if task in values:
                    completed[task] = task(completed[node])
            if task not in completed:
                completed[task] = task()
        return completed
        
if __name__=='__main__':
    dag = DAG()
    dag.add(1)
    dag.add(1, 2)
    dag.add(1, 3)
    dag.add(1, 4)
    dag.add(2, 6)
    dag.add(3, 5)
    dag.add(4, 7)
    dag.add(5, 7)
    dag.add(6, 7)
    print(dag.graph)

    dag = DAG()
    dag.add(1)
    dag.add(1, 2)
    dag.add(1, 3)
    dag.add(1, 4)
    dag.add(3, 5)
    dag.add(2, 6)
    dag.add(4, 7)
    dag.add(5, 7)
    dag.add(6, 7)
    dag.in_degrees()
    print(dag.degrees)

    dag = DAG()
    dag.add(1)
    dag.add(1, 2)
    dag.add(1, 3)
    dag.add(1, 4)
    dag.add(3, 5)
    dag.add(2, 6)
    dag.add(4, 7)
    dag.add(5, 7)
    dag.add(6, 7)
    dependencies = dag.sort()
    print(dependencies)

    dag = DAG()
    dag.add(1)
    dag.add(1, 2)
    dag.add(1, 3)
    dag.add(1, 4)
    dag.add(3, 5)
    dag.add(2, 6)
    dag.add(4, 7)
    dag.add(5, 7)
    dag.add(6, 7)
    # Add a pointer from 7 to 4, causing a cycle, which should throw an exception
    try:
        dag.add(7, 4)
    except Exception as e:
        print(e)
        
    pipeline = Pipeline()
    @pipeline.task()
    def first():
        return 20

    @pipeline.task(depends_on=first)
    def second(x):
        return x * 2

    @pipeline.task(depends_on=second)
    def third(x):
        return x // 3

    @pipeline.task(depends_on=second)
    def fourth(x):
        return x // 4

    graph = pipeline.tasks.graph
    outputs = pipeline.run()
    print(outputs)