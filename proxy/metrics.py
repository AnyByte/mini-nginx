class Metrics:
    def __init__(self):
        self.active_client_connections = 0
        self.requests_total = 0
        self.errors_total = 0
        self.timeouts_total = 0
        self.total_request_duration_seconds = 0
        self.response_status_counts = {}
        self.requests_per_upstream = {}

    def record_completed_request(self, response_status, request_duration_seconds):
        self.requests_total += 1
        self.total_request_duration_seconds += request_duration_seconds
        self.response_status_counts[response_status] = self.response_status_counts.get(response_status, 0) + 1
