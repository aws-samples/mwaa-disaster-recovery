"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from collections import defaultdict 
from typing import TypeVar, Generic
from airflow.models.baseoperator import BaseOperator

T = TypeVar('T')

class DependencyModel(Generic[T]):
    nodes: set[T]
    forward_graph: defaultdict[T, set[T]]
    reverse_graph: defaultdict[T, set[T]]

    def __init__(self) -> None:
        self.nodes = set()
        self.forward_graph = defaultdict(set)
        self.reverse_graph = defaultdict(set)
    

    def add(self, node: T):
        self.nodes.add(node)


    def add_dependency(self, dependent: T, dependencies) -> T:
        if not isinstance(dependencies, list):
            dependencies = [dependencies]

        for node in dependencies:
            self.forward_graph[dependent].add(node)
            self.reverse_graph[node].add(dependent)
        
        return dependent


    def sources(self) -> set[T]:
        sources = set()
        for node in self.nodes:
            if len(self.reverse_graph[node]) ==  0:
                sources.add(node)
        return sources


    def sinks(self) -> set[T]:
        sinks = []
        for node in self.nodes:
            if len(self.forward_graph[node]) == 0:
                sinks.append(node)
        return sinks
    

    def graph_forward(
            self, 
            start_task: BaseOperator, 
            all_tasks: dict[T, BaseOperator],
            end_task: BaseOperator
    ):
        for source in self.sources():
            start_task >> all_tasks[source]

        for node, dependencies in self.forward_graph.items():
            node_task = all_tasks[node]
            
            for dependency in dependencies:
                dependency_task = all_tasks[dependency] 
                node_task >> dependency_task
        
        for sink in self.sinks():
            all_tasks[sink] >> end_task


    def graph_reverse(
            self, 
            start_task: BaseOperator, 
            all_tasks: dict[T, BaseOperator],
            end_task: BaseOperator
    ):
        for sink in self.sinks():
            start_task >> all_tasks[sink]

        for node, dependencies in self.reverse_graph.items():
            node_task = all_tasks[node]
            
            for dependency in dependencies:
                dependency_task = all_tasks[dependency] 
                node_task >> dependency_task
        
        for source in self.sources():
            all_tasks[source] >> end_task
