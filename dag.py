import logging
from copy import deepcopy
from typing import List, Hashable, Dict, Set, Optional, Union

from task import Task
import networkx as nx

logger = logging.getLogger(__name__)


class DiGraphEx(nx.DiGraph):
    """
    Extends the DiGraph with some methods
    """

    def root_nodes(self) -> List[Hashable]:
        """
        Safely gets the root nodes
        Returns:
            the root nodes
        """
        return [node for node, degree in self.in_degree if degree == 0]

    def leaf_nodes(self) -> List[Hashable]:
        """
        Safely gets the leaf nodes
        Returns:
            the leaf nodes
        """
        return [node for node, degree in self.out_degree if degree == 0]

    def topological_sort(self) -> List[Hashable]:
        """
        Makes the simple topological sort of the graph nodes
        """
        return list(nx.topological_sort(self))


class DAG:
    def __init__(self, tasks: List[Task]):
        """
        A class representing a directed acyclic graph of tasks.

        Args:
            tasks (List[Task]): A list of Task objects that form the nodes of the graph.
        """
        self.graph_ids = DiGraphEx()
        self.exec_nodes = tasks
        self.node_dict: Dict[Hashable, Task] = {
            exec_node.name: exec_node for exec_node in self.exec_nodes
        }
        self.graph = {}

        # variables necessary for DAG construction
        self.backwards_hierarchy: Dict[Hashable, List[Hashable]] = {
            exec_node.name: exec_node.dependencies for exec_node in self.exec_nodes
        }
        self.node_dict: Dict[Hashable, Task] = {
            exec_node.name: exec_node for exec_node in self.exec_nodes
        }

        self.node_dict_by_name: Dict[str, Task] = {
            exec_node.name: exec_node for exec_node in self.exec_nodes
        }

        self._build()

    def _build(self) -> None:
        """
        Builds the graph and the sequence order for the computation.
        """
        # add nodes
        for node_id in self.backwards_hierarchy.keys():
            self.graph_ids.add_node(node_id)

        # add edges
        for node_id, dependencies in self.backwards_hierarchy.items():
            if dependencies is not None:
                edges = [(dep, node_id) for dep in dependencies]
                self.graph_ids.add_edges_from(edges)

        # set sequence order
        topological_order = self.graph_ids.topological_sort()
        # calculate the sum of priorities of all recursive children
        self.assign_recursive_children_compound_priority()

        self.exec_node_sequence = [self.node_dict[node_name] for node_name in topological_order]

    def assign_recursive_children_compound_priority(self) -> None:
        """
        Assigns a compound priority to all nodes in the graph.
        The compound priority is the sum of the priorities of all children recursively.
        """
        # Note: if there was a forward dependency recorded, this would have been much easier

        graph_ids = deepcopy(self.graph_ids)
        leaf_ids = graph_ids.leaf_nodes()

        # assign the compound priority for all the remaining nodes in the graph:
        # Priority assignment happens by epochs:
        # during every epoch, we assign the compound priority for the parents of the current leaf nodes
        # at the end of every epoch, we trim the graph from its leaf nodes;
        # hence the previous parents become the new leaf nodes
        while len(graph_ids) > 0:

            # Epoch level
            for leaf_id in leaf_ids:
                leaf_node = self.node_dict[leaf_id]

                for parent_id in self.backwards_hierarchy[leaf_id]:
                    # increment the compound_priority of the parent node by the leaf priority
                    parent_node = self.node_dict[parent_id]
                    parent_node.compound_priority += leaf_node.compound_priority

                # trim the graph from its leaf nodes
                graph_ids.remove_node(leaf_id)

            # assign the new leaf nodes
            leaf_ids = graph_ids.leaf_nodes()
