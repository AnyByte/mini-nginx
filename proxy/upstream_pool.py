import asyncio


class UpstreamPool:
    def __init__(self, server_config):
        self.upstreams = []
        self.next_upstream_index = 0
        for upstream_config in server_config["upstreams"]:
            connection_semaphore = asyncio.Semaphore(server_config["limits"]["max_conns_per_upstream"])
            self.upstreams.append((upstream_config["host"], upstream_config["port"], connection_semaphore))

    def choose_next_upstream(self):
        selected_upstream = self.upstreams[self.next_upstream_index]
        self.next_upstream_index += 1
        if self.next_upstream_index == len(self.upstreams):
            self.next_upstream_index = 0
        return selected_upstream
