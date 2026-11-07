"""Fixed-size numeric telemetry history, with bounded peak-preserving reads."""

from array import array
import math

MAX_HISTORY_SAMPLES = 604_800
MAX_HISTORY_SECONDS = 604_800
MAX_HISTORY_POINTS = 2_000
DEFAULT_HISTORY_POINTS = 600

# No Host snapshots, Session IDs or Python dictionaries are retained per point.
FIELDS = ('timestamp_us', 'rss_bytes', 'budget_bytes', 'cgroup_current_bytes',
          'persistence_retained_bytes', 'projection_bytes', 'resident_graphs')
PRESSURES = ('unknown', 'normal', 'high', 'critical')
WIDTH = len(FIELDS) + 2  # pressure code and monotonic time


class MemoryHistory:
    """Caller serializes mutation/capture; formatting runs outside its lock."""

    def __init__(self, capacity, retention_seconds):
        self.capacity, self.retention_seconds = capacity, retention_seconds
        self.data = array('d')
        self.start = self.count = 0

    @property
    def storage_bytes(self):
        return len(self.data) * self.data.itemsize

    def clear(self):
        self.data = array('d')
        self.start = self.count = 0

    def _offset(self, index):
        return ((self.start + index) % self.capacity) * WIDTH

    def expire(self, now):
        while self.count and self.data[self._offset(0) + WIDTH - 1] < now - self.retention_seconds:
            self.start = (self.start + 1) % self.capacity
            self.count -= 1

    def append(self, sample):
        if not self.data:
            self.data = array('d', [math.nan]) * (self.capacity * WIDTH)
        now = sample['monotonic_seconds']
        self.expire(now)
        host = sample.get('host') or {}
        values = (sample['timestamp_us'], sample['process'].get('rss_bytes'), sample.get('budget_bytes'),
                  (sample.get('cgroup') or {}).get('current_bytes'),
                  host.get('persistence', {}).get('retained_bytes'),
                  host.get('projections', {}).get('bytes'), host.get('resident_graphs'),
                  PRESSURES.index(sample['pressure']), now)
        offset = self._offset(self.count)
        self.data[offset:offset + WIDTH] = array('d', (math.nan if x is None else x for x in values))
        if self.count == self.capacity:
            self.start = (self.start + 1) % self.capacity
        else:
            self.count += 1

    def capture(self, *, history, window_seconds, now):
        """Copy packed numbers, never expand a full day into Python objects."""
        self.expire(now)
        begin = max(0, self.count - history) if history else 0
        if window_seconds is not None:
            lo, hi = begin, self.count
            while lo < hi:
                mid = (lo + hi) // 2
                if self.data[self._offset(mid) + WIDTH - 1] < now - window_seconds:
                    lo = mid + 1
                else:
                    hi = mid
            begin = lo
        if not history and window_seconds is None:
            begin = self.count
        count = self.count - begin
        if not count:
            return array('d'), self.count
        offset = self._offset(begin)
        size = count * WIDTH
        result = self.data[offset:min(offset + size, len(self.data))]
        if len(result) < size:
            result.extend(self.data[:size - len(result)])
        return result, self.count


def history_points(data, limit):
    """Keep endpoints and RSS extrema in time buckets, in capture order."""
    count = len(data) // WIDTH
    if count <= limit:
        selected = range(count)
    elif limit < 4:
        selected = (0, count - 1)
    else:
        selected = [0]
        buckets = (limit - 2) // 2
        for bucket in range(buckets):
            begin = 1 + (count - 2) * bucket // buckets
            end = 1 + (count - 2) * (bucket + 1) // buckets
            low = high = begin
            for index in range(begin, end):
                rss = data[index * WIDTH + 1]
                if math.isnan(rss):
                    continue
                if math.isnan(data[low * WIDTH + 1]) or rss < data[low * WIDTH + 1]:
                    low = index
                if math.isnan(data[high * WIDTH + 1]) or rss > data[high * WIDTH + 1]:
                    high = index
            selected.extend(sorted({low, high}))
        selected.append(count - 1)
    result = []
    for index in selected:
        offset = index * WIDTH
        point = {name: None if math.isnan(data[offset + column]) else int(data[offset + column])
                 for column, name in enumerate(FIELDS)}
        point['pressure'] = PRESSURES[int(data[offset + len(FIELDS)])]
        result.append(point)
    return result
