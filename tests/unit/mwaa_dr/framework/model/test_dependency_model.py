# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

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

from sure import expect

import datetime
import pendulum

from airflow import DAG
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from airflow.operators.bash import BashOperator

from assets.dags.mwaa_dr.framework.model.dependency_model import DependencyModel

class TestDependencyModel:
    def test_construction(self):
        model = DependencyModel()
        expect(model.nodes).to.be.empty
        expect(model.forward_graph).to.be.empty
        expect(model.reverse_graph).to.be.empty
        
    def test_add(self):
        model = DependencyModel()

        model.add("test1")
        expect(model.nodes).to.have.length_of(1)
        expect(model.nodes).to.contain("test1")

        model.add("test2")
        expect(model.nodes).to.have.length_of(2)
        expect(model.nodes).to.contain("test2")


    def test_single_source_single_sink(self):
        model = DependencyModel()

        model.add("source")
        model.add("node1")
        model.add("node2")
        model.add("node3")
        model.add("sink")

        # source -> [node1, node2] -> node3 -> sink
        source = model.add_dependency("source", ["node1", "node2"])
        node1 = model.add_dependency("node1", "node3")
        node2= model.add_dependency("node2", "node3")
        node3 = model.add_dependency("node3", "sink")

        expect(source).to.equal('source')
        expect(node3).to.equal('node3')

        expect(model.sources()).to.have.length_of(1)
        expect(model.sources()).to.contain(source)

        expect(model.sinks()).to.have.length_of(1)
        expect(model.sinks()).to.contain('sink')

        expect(model.forward_graph[source]).to.have.length_of(2)
        expect(model.forward_graph[source]).to.contain(node1)
        expect(model.forward_graph[source]).to.contain(node2)
        expect(model.forward_graph[node1]).to.have.length_of(1)
        expect(model.forward_graph[node1]).to.contain(node3)
        expect(model.forward_graph[node2]).to.have.length_of(1)
        expect(model.forward_graph[node2]).to.contain(node3)


        expect(model.reverse_graph['sink']).to.have.length_of(1)
        expect(model.reverse_graph['sink']).to.contain(node3)
        expect(model.reverse_graph[node3]).to.have.length_of(2)
        expect(model.reverse_graph[node3]).to.contain(node1)
        expect(model.reverse_graph[node3]).to.contain(node2)
        expect(model.reverse_graph[node2]).to.have.length_of(1)
        expect(model.reverse_graph[node2]).to.contain(source)


    def test_multiple_sources_multiple_sinks(self):
        model = DependencyModel()

        source1 = model.add("source1")
        souce2 = model.add("source2")
        node = model.add("node")
        sink1 = model.add("sink1")
        sink2 = model.add("sink2")

        # [source1, source2] -> node -> [sink1, sink2]
        model.add_dependency(source1, node)
        model.add_dependency(souce2, node)
        model.add_dependency(node, sink1)
        model.add_dependency(node, sink2)

        expect(model.sources()).to.have.length_of(2)
        expect(model.sources()).to.contain(source1)
        expect(model.sources()).to.contain(souce2)
        expect(model.sinks()).to.have.length_of(2)
        expect(model.sinks()).to.contain(sink1)
        expect(model.sinks()).to.contain(sink2)

        expect(model.forward_graph[source1]).to.have.length_of(1)
        expect(model.forward_graph[source1]).to.contain(node)
        expect(model.forward_graph[souce2]).to.have.length_of(1)
        expect(model.forward_graph[souce2]).to.contain(node)
        expect(model.forward_graph[node]).to.have.length_of(2)
        expect(model.forward_graph[node]).to.contain(sink1)
        expect(model.forward_graph[node]).to.contain(sink2)

        expect(model.reverse_graph[sink1]).to.have.length_of(1)
        expect(model.reverse_graph[sink1]).to.contain(node)
        expect(model.reverse_graph[sink2]).to.have.length_of(1)
        expect(model.reverse_graph[sink2]).to.contain(node)
        expect(model.reverse_graph[node]).to.have.length_of(2)
        expect(model.reverse_graph[node]).to.contain(source1)
        expect(model.reverse_graph[node]).to.contain(souce2)


    def test_apply(self):
        model = DependencyModel()

        source1 = model.add("source1")
        souce2 = model.add("source2")
        node = model.add("node")
        sink1 = model.add("sink1")
        sink2 = model.add("sink2")

        # [source1, source2] -> node -> [sink1, sink2]
        model.add_dependency(source1, node)
        model.add_dependency(souce2, node)
        model.add_dependency(node, sink1)
        model.add_dependency(node, sink2)

        start_time = pendulum.datetime(2021, 9, 13, tz="UTC")
        end_time = start_time + datetime.timedelta(days=1)

        dag = DAG('test_dag', start_date=start_time, schedule="@daily")
        start_task = BashOperator(task_id="start", bash_command="echo start", dag=dag)
        end_task = BashOperator(task_id="end", bash_command="echo end", dag=dag)

        node_to_task = dict()
        for n in model.nodes:
            node_to_task[n] = BashOperator(task_id=n, bash_command=f"echo {n}", dag=dag)
        

        model.apply(start_task, node_to_task, end_task)
        
        expect(start_task.downstream_list).to.have.length_of(2)
        expect(start_task.downstream_list).to.contain(node_to_task[source1])
        expect(start_task.downstream_list).to.contain(node_to_task[souce2])

        expect(node_to_task[source1].downstream_list).to.have.length_of(1)
        expect(node_to_task[source1].downstream_list).to.contain(node_to_task[node])
        expect(node_to_task[souce2].downstream_list).to.have.length_of(1)
        expect(node_to_task[souce2].downstream_list).to.contain(node_to_task[node])

        expect(node_to_task[node].downstream_list).to.have.length_of(2)
        expect(node_to_task[node].downstream_list).to.contain(node_to_task[sink1])
        expect(node_to_task[node].downstream_list).to.contain(node_to_task[sink2])

        expect(end_task.upstream_list).to.have.length_of(2)
        expect(end_task.upstream_list).to.contain(node_to_task[sink1])        
        expect(end_task.upstream_list).to.contain(node_to_task[sink2])
