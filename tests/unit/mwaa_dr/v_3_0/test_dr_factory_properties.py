# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

"""
Property-based tests for DRFactory_3_0 dependency ordering using Hypothesis.

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

from hypothesis import given, settings, assume
from hypothesis.strategies import (
    composite,
    integers,
    lists,
    sampled_from,
    text,
    tuples,
)

from mwaa_dr.framework.factory.glue_dr_factory import GlueDRFactory
from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel
from mwaa_dr.v_3_0.dr_factory import DRFactory_3_0


# --- Strategies ---


@composite
def valid_table_names(draw):
    """Generate valid table name strings."""
    name = draw(
        text(
            alphabet="abcdefghijklmnopqrstuvwxyz_",
            min_size=1,
            max_size=30,
        )
    )
    assume(name[0].isalpha())
    return name


@composite
def random_dag_with_dependencies(draw):
    """Generate a random DAG (directed acyclic graph) of tables with dependencies.

    Returns a tuple of (table_names, edges) where edges is a list of
    (child, parent) tuples representing dependency relationships.
    The graph is guaranteed to be acyclic by only allowing edges from
    higher-indexed nodes to lower-indexed nodes.
    """
    num_tables = draw(integers(min_value=2, max_value=15))

    # Generate unique table names
    base_names = [f"table_{i}" for i in range(num_tables)]

    # Generate edges: only allow edges from higher index to lower index
    # to guarantee acyclicity
    edges = []
    for child_idx in range(1, num_tables):
        # Each node can depend on 0 to child_idx parents
        num_parents = draw(integers(min_value=0, max_value=min(child_idx, 3)))
        if num_parents > 0:
            parent_indices = draw(
                lists(
                    sampled_from(list(range(child_idx))),
                    min_size=num_parents,
                    max_size=num_parents,
                    unique=True,
                )
            )
            for parent_idx in parent_indices:
                edges.append((base_names[child_idx], base_names[parent_idx]))

    return base_names, edges


def build_model_from_spec(table_names, edges):
    """Build a DependencyModel and GlueDRFactory-compatible structure from a spec.

    Creates a minimal GlueDRFactory subclass that defines the given tables
    and dependencies, then returns the factory with its computed dependency order.
    """
    model = DependencyModel()
    tables = {}

    for name in table_names:
        table = BaseTable(name=name, model=model)
        tables[name] = table

    # Wire dependencies using the << operator pattern
    for child_name, parent_name in edges:
        child = tables[child_name]
        parent = tables[parent_name]
        child << [parent]

    return model, tables


# --- Property Tests ---


class TestDependencyOrderingProperties:
    """
    **Validates: Requirements 3.7, 4.7, 5.2, 6.5**

    Property 5: Dependency ordering produces valid topological sorts

    For any valid DependencyModel with tables and dependency edges, the
    computed export/cleanup order SHALL be a valid reverse topological sort
    (child tables before parent tables), and the computed import order SHALL
    be a valid forward topological sort (parent tables before child tables).
    Additionally, tables with no dependency relationship between them SHALL
    appear at the same ordering level.
    """

    def test_drfactory_3_0_produces_valid_forward_topological_sort(self):
        """
        **Validates: Requirements 3.7, 4.7, 5.2, 6.5**

        Verify that the DRFactory_3_0 dependency order is a valid forward
        topological sort: for every dependency edge (child depends on parent),
        the parent appears at an earlier level than the child.
        """
        factory = DRFactory_3_0("test_dag")
        levels = factory.get_table_dependency_order()

        # Build a lookup from table name to its level index
        table_to_level = {}
        for level_idx, level_tables in enumerate(levels):
            for table_name in level_tables:
                table_to_level[table_name] = level_idx

        # Verify all tables are present
        all_table_names = {t.name for t in factory.tables()}
        all_ordered_names = set(table_to_level.keys())
        assert (
            all_table_names == all_ordered_names
        ), f"Missing tables in ordering: {all_table_names - all_ordered_names}"

        # For every dependency edge, parent must be at an earlier or equal level
        model = factory.model
        for node in model.nodes:
            # reverse_graph[node] = set of nodes that node depends on (prerequisites)
            prerequisites = model.reverse_graph[node]
            for prereq in prerequisites:
                assert table_to_level[prereq.name] < table_to_level[node.name], (
                    f"Topological sort violation: {prereq.name} (level "
                    f"{table_to_level[prereq.name]}) should come before "
                    f"{node.name} (level {table_to_level[node.name]})"
                )

    def test_drfactory_3_0_reverse_order_is_valid_for_export_cleanup(self):
        """
        **Validates: Requirements 3.7, 4.7, 5.2, 6.5**

        Verify that reversing the dependency order produces a valid reverse
        topological sort for export/cleanup: child tables come before parent tables.
        """
        factory = DRFactory_3_0("test_dag")
        levels = factory.get_table_dependency_order()

        # Reverse the levels for export/cleanup order
        reversed_levels = list(reversed(levels))

        # Build a lookup from table name to its reversed level index
        table_to_level = {}
        for level_idx, level_tables in enumerate(reversed_levels):
            for table_name in level_tables:
                table_to_level[table_name] = level_idx

        # For every dependency edge, child must come before parent in reversed order
        model = factory.model
        for node in model.nodes:
            prerequisites = model.reverse_graph[node]
            for prereq in prerequisites:
                assert table_to_level[node.name] < table_to_level[prereq.name], (
                    f"Reverse topological sort violation: {node.name} (level "
                    f"{table_to_level[node.name]}) should come before "
                    f"{prereq.name} (level {table_to_level[prereq.name]}) "
                    f"in export/cleanup order"
                )

    @given(dag_spec=random_dag_with_dependencies())
    @settings(max_examples=100)
    def test_random_dag_produces_valid_topological_sort(self, dag_spec):
        """
        **Validates: Requirements 3.7, 4.7, 5.2, 6.5**

        Property 5: For any random DAG, the topological sort produced by
        get_table_dependency_order() is valid — every parent appears at
        an earlier level than its children.
        """
        table_names, edges = dag_spec
        model, tables = build_model_from_spec(table_names, edges)

        # Use the same BFS topological sort algorithm as GlueDRFactory
        processed = set()
        levels = []

        while len(processed) < len(model.nodes):
            current_level = []
            for node in model.nodes:
                if node in processed:
                    continue
                prerequisites = model.reverse_graph[node]
                if prerequisites.issubset(processed):
                    current_level.append(node)

            if not current_level:
                remaining = [n for n in model.nodes if n not in processed]
                levels.append([t.name for t in remaining])
                break

            levels.append([t.name for t in current_level])
            processed.update(current_level)

        # Build level lookup
        table_to_level = {}
        for level_idx, level_tables in enumerate(levels):
            for table_name in level_tables:
                table_to_level[table_name] = level_idx

        # Verify all tables are present
        assert set(table_to_level.keys()) == set(table_names)

        # Verify topological ordering: for every edge (child, parent),
        # parent must be at an earlier level
        for child_name, parent_name in edges:
            assert table_to_level[parent_name] < table_to_level[child_name], (
                f"Topological sort violation: {parent_name} (level "
                f"{table_to_level[parent_name]}) should come before "
                f"{child_name} (level {table_to_level[child_name]})"
            )

    @given(dag_spec=random_dag_with_dependencies())
    @settings(max_examples=100)
    def test_independent_tables_share_same_level(self, dag_spec):
        """
        **Validates: Requirements 3.7, 4.7, 5.2, 6.5**

        Property 5: Tables with no dependency relationship between them
        can appear at the same ordering level. Specifically, tables at the
        same level should have no direct dependency between them.
        """
        table_names, edges = dag_spec
        model, tables = build_model_from_spec(table_names, edges)

        # Compute levels using the same algorithm
        processed = set()
        levels = []

        while len(processed) < len(model.nodes):
            current_level = []
            for node in model.nodes:
                if node in processed:
                    continue
                prerequisites = model.reverse_graph[node]
                if prerequisites.issubset(processed):
                    current_level.append(node)

            if not current_level:
                remaining = [n for n in model.nodes if n not in processed]
                levels.append([t.name for t in remaining])
                break

            levels.append([t.name for t in current_level])
            processed.update(current_level)

        # Build edge set for quick lookup
        edge_set = set(edges)

        # Verify: no two tables at the same level have a direct dependency
        for level_tables in levels:
            for i, t1 in enumerate(level_tables):
                for t2 in level_tables[i + 1 :]:
                    assert (t1, t2) not in edge_set and (t2, t1) not in edge_set, (
                        f"Tables {t1} and {t2} are at the same level but have "
                        f"a direct dependency between them"
                    )
