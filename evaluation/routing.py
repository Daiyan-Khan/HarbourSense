"""Offline discrete-event transport workload using the production route planners.

This calls SmartRoutePlanner.compute_path and its existing unweighted BFS, but
does not run manager/simulator/MQTT. Results are simulated transport outcomes,
not a benchmark of full-stack shipment handling, network latency or capacity.
"""
import argparse
import heapq
import json
import logging
import random
import statistics
import sys
import time
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path

from common import ROOT, provenance, sha256, write_json

sys.path.insert(0, str(ROOT / "python-backend"))
from traffic_analyzer import GRAPH_LIST, SmartRoutePlanner, parse_graph, validate_path_adjacency

logging.getLogger("TrafficAnalyzer").disabled = True


def make_workload(config, seed):
    rng = random.Random(seed)
    now = 0
    jobs = []
    for index in range(config["shipments"]):
        now += rng.randint(*config["arrivalGapSeconds"])
        jobs.append({"id": f"{seed}-{index:03}", "arrivalSeconds": now, "origin": rng.choice(config["origins"]), "destination": rng.choice(config["destinations"])})
    return jobs


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    return ordered[lower] + (ordered[min(lower + 1, len(ordered) - 1)] - ordered[lower]) * (position - lower)


def simulate(graph, workload, config, strategy):
    if strategy not in ("weighted", "bfs"):
        raise ValueError("Unknown strategy")
    planner = SmartRoutePlanner(graph)
    jobs = {item["id"]: {**item, "status": "scheduled", "routeChanges": 0, "hops": 0, "edgeQueueSeconds": 0, "travelPaths": {"pickup": [], "delivery": []}} for item in workload}
    fleet = [{"id": index, "node": config["fleetStart"], "job": None, "phase": None, "remaining": []} for index in range(config["fleetSize"])]
    waiting = deque()
    events = []
    sequence = 0
    edge_free = {}
    trace = []
    bottleneck_edges = {tuple(sorted(pair)) for pair in config["bottleneck"]["edges"]}

    def schedule(second, kind, identifier):
        nonlocal sequence
        sequence += 1
        heapq.heappush(events, (second, sequence, kind, identifier))

    def penalty(first, second, now):
        fault = config["bottleneck"]
        return fault["additionalHopSeconds"] if fault["startSeconds"] <= now < fault["endSeconds"] and tuple(sorted((first, second))) in bottleneck_edges else 0

    def fail(vehicle, job, now, reason):
        job.update(status="failed", failedAtSeconds=now, failureReason=reason)
        trace.append({"simulatedSeconds": now, "event": "failed", "jobId": job["id"], "reason": reason})
        vehicle.update(job=None, phase=None, remaining=[])

    def advance(vehicle, now):
        job = jobs[vehicle["job"]]
        target = job["origin"] if vehicle["phase"] == "pickup" else job["destination"]
        if vehicle["node"] == target:
            if vehicle["phase"] == "pickup":
                vehicle.update(phase="delivery", remaining=[])
                job["pickedUpSeconds"] = now + config["handlingSeconds"]
                schedule(now + config["handlingSeconds"], "advance", vehicle["id"])
            else:
                schedule(now + config["handlingSeconds"], "complete", vehicle["id"])
            return
        if job["hops"] >= 500:
            fail(vehicle, job, now, "500-hop safety limit")
            return
        loads = dict(Counter(item["node"] for item in fleet if item["job"] is not None))
        congestion = {}
        for first, node in graph.items():
            for second in node["neighbors"].values():
                queue_delay = max(0, edge_free.get(tuple(sorted((first, second))), 0) - now)
                congestion[f"{first}-{second}"] = {"ratio": (penalty(first, second, now) + queue_delay) / config["hopSeconds"]}
        if strategy == "weighted":
            path = planner.compute_path(vehicle["node"], target, loads, congestion, predicted_loads={})
        else:
            path = planner._bfs_shortest_path(vehicle["node"], target)
        if not path or len(path) < 2 or not validate_path_adjacency(path, planner):
            fail(vehicle, job, now, "No valid adjacent route")
            return
        if vehicle["remaining"] and vehicle["remaining"] != path:
            job["routeChanges"] += 1
        vehicle["remaining"] = path[1:]
        first, second = path[:2]
        key = tuple(sorted((first, second)))
        departure = max(now, edge_free.get(key, 0))
        arrival = departure + config["hopSeconds"] + penalty(first, second, departure)
        edge_free[key] = arrival
        job["edgeQueueSeconds"] += departure - now
        job["hops"] += 1
        job["travelPaths"][vehicle["phase"]].append([first, second])
        vehicle["nextNode"] = second
        trace.append({"simulatedSeconds": now, "event": "hop", "jobId": job["id"], "vehicleId": vehicle["id"], "phase": vehicle["phase"], "path": path, "departureSeconds": departure, "arrivalSeconds": arrival})
        schedule(arrival, "arrive", vehicle["id"])

    def assign(now):
        for vehicle in fleet:
            if vehicle["job"] is None and waiting:
                job = jobs[waiting.popleft()]
                job.update(status="active", assignedSeconds=now)
                vehicle.update(job=job["id"], phase="pickup", remaining=[])
                trace.append({"simulatedSeconds": now, "event": "assigned", "jobId": job["id"], "vehicleId": vehicle["id"]})
                advance(vehicle, now)

    for job in jobs.values():
        schedule(job["arrivalSeconds"], "shipment", job["id"])
    while events:
        now, _, kind, identifier = heapq.heappop(events)
        if now > config["horizonSeconds"]:
            break
        if kind == "shipment":
            jobs[identifier]["status"] = "queued"
            waiting.append(identifier)
        else:
            vehicle = fleet[identifier]
            if kind == "complete":
                job = jobs[vehicle["job"]]
                job.update(status="completed", completedSeconds=now, deliverySeconds=now - job["arrivalSeconds"])
                trace.append({"simulatedSeconds": now, "event": "completed", "jobId": job["id"]})
                vehicle.update(job=None, phase=None, remaining=[])
            else:
                if kind == "arrive":
                    vehicle["node"] = vehicle.pop("nextNode")
                advance(vehicle, now)
        assign(now)
    completed = [job for job in jobs.values() if job["status"] == "completed"]
    durations = [job["deliverySeconds"] for job in completed]
    failed = sum(job["status"] == "failed" for job in jobs.values())
    return {
        "strategy": strategy,
        "completed": len(completed), "failed": failed,
        "unfinished": len(jobs) - len(completed) - failed,
        "throughputPerSimulatedMinute": len(completed) * 60 / config["horizonSeconds"],
        "deliverySecondsAmongCompleted": {"median": statistics.median(durations) if durations else None, "p95": percentile(durations, 0.95), "min": min(durations) if durations else None, "max": max(durations) if durations else None},
        "routeChanges": sum(job["routeChanges"] for job in jobs.values()),
        "jobs": list(jobs.values()),
    }, trace


def evaluate(config_path, output_dir, saved_inputs=None):
    started = datetime.now(timezone.utc).isoformat()
    timer = time.perf_counter()
    config = json.loads(config_path.read_text(encoding="utf-8"))["routing"]
    if saved_inputs:
        saved = json.loads(saved_inputs.read_text(encoding="utf-8"))
        config, graph, workloads = saved["config"], saved["graph"], saved["workloads"]
    else:
        graph = parse_graph(GRAPH_LIST)
        workloads = [{"seed": seed, "shipments": make_workload(config, seed)} for seed in config["seeds"]]
    inputs_path = output_dir / "routing-inputs.json"
    write_json(inputs_path, {"schemaVersion": 1, "graph": graph, "config": config, "workloads": workloads})
    runs = []
    trace_path = output_dir / "routing-events.jsonl"
    with trace_path.open("w", encoding="utf-8") as handle:
        for workload in workloads:
            for strategy in ("weighted", "bfs"):
                result, trace = simulate(graph, workload["shipments"], config, strategy)
                result["seed"] = workload["seed"]
                runs.append(result)
                for event in trace:
                    handle.write(json.dumps({"seed": workload["seed"], "strategy": strategy, **event}) + "\n")
    summaries = {}
    for strategy in ("weighted", "bfs"):
        selected = [run for run in runs if run["strategy"] == strategy]
        summaries[strategy] = {}
        for metric in ("completed", "failed", "unfinished", "throughputPerSimulatedMinute", "routeChanges"):
            values = [run[metric] for run in selected]
            summaries[strategy][metric] = {"mean": statistics.mean(values), "sampleStdDev": statistics.stdev(values), "min": min(values), "max": max(values)}
        for metric in ("median", "p95"):
            values = [run["deliverySecondsAmongCompleted"][metric] for run in selected if run["deliverySecondsAmongCompleted"][metric] is not None]
            summaries[strategy][f"{metric}DeliverySecondsAmongCompleted"] = {"mean": statistics.mean(values) if values else None, "sampleStdDev": statistics.stdev(values) if len(values) > 1 else None}
    results = {
        "schemaVersion": 1, "experiment": "Offline discrete-event transport workload using production weighted routing and unweighted BFS",
        "config": config, "summaries": summaries, "runs": runs,
        "inputsSha256": sha256(inputs_path), "eventsSha256": sha256(trace_path),
        "provenance": provenance(["python-backend/traffic_analyzer.py", "evaluation/routing.py", "evaluation/common.py", "evaluation/config.json"], started, time.perf_counter() - timer),
        "limitations": [
            "Uses production compute_path and _bfs_shortest_path with the production fallback graph; it does not execute manager, port-sim, MQTT, MongoDB, cranes, or warehouse capacity logic.",
            "Four abstract transport vehicles share bidirectional edges with FIFO single-vehicle service, four-second hops and two-second pickup/drop-off handling. This is an evaluation model, not a calibrated port.",
            "The scripted bottleneck adds travel time, independently of route scoring. Weighted routing observes current queued edge time and injected delay; BFS ignores those signals.",
            "Routes are recomputed at each hop. Route changes count changed remaining plans after the first plan of a phase. Predicted loads are empty; observed loads count working vehicles' last reached nodes.",
            "Delivery time runs from requested arrival to completed drop-off and includes fleet queues and empty pickup travel. Median and p95 exclude unfinished/failed deliveries, whose counts are reported alongside.",
            "Throughput uses the entire fixed 600-second simulated horizon. Wall-clock execution time is evaluator runtime, not API latency or real system throughput.",
            "Workloads and graph are identical between strategies for each seed. Aggregate variation is sample standard deviation across five seeds; no cherry-picked runs or full-stack performance claim.",
        ],
    }
    write_json(output_dir / "routing-results.json", results)
    print(json.dumps(summaries, indent=2))
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "evaluation/config.json")
    parser.add_argument("--output", type=Path, default=ROOT / "evaluation/results")
    parser.add_argument("--inputs", type=Path, help="Replay saved graph/config/workloads JSON")
    args = parser.parse_args()
    evaluate(args.config, args.output, args.inputs)
