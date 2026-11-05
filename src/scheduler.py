class WorkloadAwareScheduler:
    """Choose the worker with the lowest queue-pressure + response-time score."""
    def choose(self, workers):
        if not workers: raise RuntimeError("No active workers")
        return min(workers, key=lambda w: (w["outstanding"] / max(1,w["capacity"]) + w["ewma_seconds"], w["outstanding"], w["id"]))
