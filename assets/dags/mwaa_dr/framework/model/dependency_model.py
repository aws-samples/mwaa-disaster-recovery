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
from typing import Generic, TypeVar

from airflow.models.baseoperator import BaseOperator

T = TypeVar("T")


class DependencyModel(Generic[T]):
    """
    A generic class to represent a dependency model between nodes of type T.

    This class maintains a directed acyclic graph (DAG) of dependencies between nodes.
    It provides methods to add nodes, add dependencies between nodes, and retrieve sources
    (nodes with no dependencies) and sinks (nodes with no dependents).

    Additionally, it provides a method to apply the dependency model to Airflow tasks,
    creating the necessary dependencies between the tasks based on the dependency model.

    Attributes:
        nodes (set[T]): A set of all nodes in the dependency model.
        forward_graph (defaultdict[T, set[T]]): A dictionary mapping each node to the set of
            nodes that depend on it.
        reverse_graph (defaultdict[T, set[T]]): A dictionary mapping each node to the set of
            nodes it depends on.
    """

    nodes: set[T]
    forward_graph: defaultdict[T, set[T]]
    reverse_graph: defaultdict[T, set[T]]

    def __init__(self) -> None:
        self.nodes = set()
        self.forward_graph = defaultdict(set)
        self.reverse_graph = defaultdict(set)

    def add(self, node: T) -> T:
        """
        Add a new node to the dependency model.

        Args:
            node (T): The node to be added.

        Returns:
            T: The added node.
        """
        self.nodes.add(node)
        return node

    def add_dependency(self, dependent: T, dependencies) -> T:
        """
        Add a dependency between nodes in the dependency model.

        Args:
            dependent (T): The node that depends on the dependencies.
            dependencies (T or list[T]): The node(s) that the dependent node depends on.

        Returns:
            T: The dependent node.
        """
        if not isinstance(dependencies, list):
            dependencies = [dependencies]

        for node in dependencies:
            self.forward_graph[dependent].add(node)
            self.reverse_graph[node].add(dependent)

        return dependent

    def sources(self) -> set[T]:
        """
        Get the set of source nodes in the dependency model (nodes with no dependents).

        Returns:
            set[T]: The set of source nodes.
        """
        sources = set()
        for node in self.nodes:
            if not self.reverse_graph[node]:
                sources.add(node)
        return sources

    def sinks(self) -> set[T]:
        """
        Get the set of sink nodes in the dependency model (nodes with no dependencies).

        Returns:
            set[T]: The set of sink nodes.
        """
        sinks = []
        for node in self.nodes:
            if not self.forward_graph[node]:
                sinks.append(node)
        return sinks

    def apply(
        self,
        start_task: BaseOperator,
        all_tasks: dict[T, BaseOperator],
        end_task: BaseOperator,
    ):
        """
        Apply the dependency model to Airflow tasks, creating the necessary dependencies
        between the tasks based on the dependency model.

        Args:
            start_task (BaseOperator): The start task in the Airflow DAG.
            all_tasks (dict[T, BaseOperator]): A dictionary mapping nodes to their corresponding
                Airflow tasks.
            end_task (BaseOperator): The end task in the Airflow DAG.
        """
        for source in self.sources():
            start_task >> all_tasks[source]

        for node, dependencies in self.forward_graph.items():
            node_task = all_tasks[node]

            for dependency in dependencies:
                dependency_task = all_tasks[dependency]
                node_task >> dependency_task

        for sink in self.sinks():
            all_tasks[sink] >> end_task

    def search(self, field_name: str, value) -> T:
        """
        Search for a node in the dependency model based on a field value.
        This is a helper for testing dependency.

        Args:
            field_name (str): The name of the field to search on.
            value: The value of the field to search for.

        Returns:
            T: The node with the specified field value, or None if not found.
        """
        for node in self.nodes:
            if getattr(node, field_name, None) == value:
                return node
        return None

    def dependencies(self, node: T) -> set[T]:
        """
        Get the set of dependencies for a given node in the dependency model.

        Args:
            node (T): The node for which to retrieve the dependencies.

        Returns:
            set[T]: The set of nodes that the given node depends on.
        """
        return self.forward_graph[node] or set[T]()

    def dependents(self, node: T) -> set[T]:
        """
        Get the set of dependents for a given node in the dependency model.

        Args:
            node (T): The node for which to retrieve the dependents.

        Returns:
            set[T]: The set of nodes that depend on the given node.
        """
        return self.reverse_graph[node] or set[T]()

    def __eq__(self, other):
        """
        Check if two DependencyModel instances are equal.

        Args:
            other (DependencyModel): The other DependencyModel instance to compare with.

        Returns:
            bool: True if the two instances are equal, False otherwise.
        """
        if self is other:
            return True

        if not isinstance(other, DependencyModel):
            return False

        return (
            self.nodes == other.nodes
            and self.forward_graph == other.forward_graph
            and self.reverse_graph == other.reverse_graph
        )

    def __hash__(self):
        """
        Raises unhasable error as a DependencyModel object  is mutable.
        """
        raise TypeError("Unhasable type: DepdendencyModel")
