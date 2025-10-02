import heapq
from math import inf
from pymongo import MongoClient

class RoutePlanner:
    def __init__(self, graph):
        self.graph = graph  # dict from db.graph.find_one()

    def compute_path(self, start, end, node_loads, capacity_threshold=0.8):
        distances = {node: inf for node in self.graph['nodes']}
        distances[start] = 0
        previous = {node: None for node in self.graph['nodes']}
        pq = [(0, start)]

        while pq:
            dist, current = heapq.heappop(pq)
            if dist > distances[current]:
                continue
            neighbors = self.graph['nodes'][current].get('neighbors', {})
            for neighbor, weight in neighbors.items():
                load_factor = node_loads.get(neighbor, 0) / capacity_threshold
                adjusted_weight = weight * (1 + load_factor)
                alt = dist + adjusted_weight
                if alt < distances[neighbor]:
                    distances[neighbor] = alt
                    previous[neighbor] = current
                    heapq.heappush(pq, (alt, neighbor))

        # Reconstruct path
        path = []
        current = end
        while current is not None:
            path.append(current)
            current = previous[current]
        path.reverse()
        return path if path and path[0] == start else None

    async def replan_if_needed(self, edge_id, db):
        edge = db['edges'].find_one({'id': edge_id})
        if not edge or 'nextNode' not in edge:
            return

        # Calculate current node loads
        node_loads = {}
        edges = list(db['edges'].find({}))
        for e in edges:
            loc = e.get('currentLocation')
            if loc:
                node_loads[loc] = node_loads.get(loc, 0) + 1

        next_node = edge['nextNode']
        capacity = self.graph['nodes'].get(next_node, {}).get('capacity', 5)  # Default max 5
        if node_loads.get(next_node, 0) >= capacity:
            new_path = self.compute_path(edge['currentLocation'], edge['destinationNode'], node_loads)
            if new_path:
                db['edges'].update_one({'id': edge_id}, {'$set': {'path': new_path, 'nextNode': new_path[1] if len(new_path) > 1 else None}})
            else:
                db['edges'].update_one({'id': edge_id}, {'$set': {'state': 'waiting'}})
