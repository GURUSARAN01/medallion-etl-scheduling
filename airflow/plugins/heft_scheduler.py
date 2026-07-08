from __future__ import annotations

from functools import lru_cache
from typing import Dict, Iterable, Tuple

import networkx as nx


Dependency = Tuple[str, str]


def calculate_heft_priority_weights(
    task_durations: Dict[str, float],
    dependencies: Iterable[Dependency],
    *,
    communication_cost_s: float = 0.0,
    scale: int = 100,
) -> tuple[Dict[str, float], Dict[str, int]]:
    """
    Calculates HEFT upward ranks and converts them into Airflow priority weights.

    HEFT upward rank:
        rank_u(task) = computation_cost(task)
                     + max(rank_u(successor) + communication_cost)

    In this local Airflow experiment, communication cost is set to 0 because
    all simulated tasks run inside the same Docker-based execution environment.

    Higher upward rank means higher scheduling priority.
    """
    graph = nx.DiGraph()

    for task_id, duration in task_durations.items():
        graph.add_node(task_id, duration=float(duration))

    for upstream, downstream in dependencies:
        if upstream not in task_durations:
            raise ValueError(f"Unknown upstream task in dependency: {upstream}")
        if downstream not in task_durations:
            raise ValueError(f"Unknown downstream task in dependency: {downstream}")
        graph.add_edge(upstream, downstream)

    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError("HEFT requires a directed acyclic graph, but a cycle was found.")

    @lru_cache(maxsize=None)
    def upward_rank(task_id: str) -> float:
        successors = list(graph.successors(task_id))
        own_cost = graph.nodes[task_id]["duration"]

        if not successors:
            return own_cost

        max_successor_rank = max(
            communication_cost_s + upward_rank(successor)
            for successor in successors
        )

        return own_cost + max_successor_rank

    ranks = {
        task_id: round(upward_rank(task_id), 6)
        for task_id in graph.nodes
    }

    priority_weights = {
        task_id: max(1, int(round(rank * scale)))
        for task_id, rank in ranks.items()
    }

    return ranks, priority_weights


def format_heft_table(
    ranks: Dict[str, float],
    priority_weights: Dict[str, int],
) -> str:
    """
    Creates a readable HEFT ranking table for Airflow logs.
    """
    rows = []

    for task_id in sorted(ranks, key=ranks.get, reverse=True):
        rows.append(
            f"{task_id:30s} rank={ranks[task_id]:8.3f} "
            f"priority_weight={priority_weights[task_id]}"
        )

    return "\n".join(rows)