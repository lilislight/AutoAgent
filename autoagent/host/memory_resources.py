"""Portable physical capacity sampling and optional Linux cgroup headroom."""

from dataclasses import dataclass
from pathlib import Path
import re

FALLBACK_BUDGET = 512 * 1024**2


def _integer(path):
    try:
        value = int(path.read_text().strip())
        return value if value >= 0 else None
    except (OSError, ValueError):
        return None


def system_memory():
    """MemAvailable includes reclaimable memory; MemFree is not a substitute."""
    values = {}
    try:
        for line in Path('/proc/meminfo').read_text().splitlines():
            key, _, text = line.partition(':')
            if key in ('MemTotal', 'MemAvailable'):
                try:
                    values[key] = max(0, int(text.split()[0])) * 1024
                except (ValueError, IndexError):
                    pass
    except OSError:
        pass
    total = values.get('MemTotal') or None
    if total is None:
        import psutil
        try:
            sample = psutil.virtual_memory()
            return sample.total or None, sample.available
        except (OSError, psutil.Error):
            return None, None
    return total, values.get('MemAvailable')


@dataclass(frozen=True)
class MemoryController:
    directory: Path
    mount: Path
    unified: bool

    def ancestors(self):
        directory = self.directory
        while True:
            yield directory
            if directory == self.mount:
                return
            directory = directory.parent


def memory_controller():
    """Resolve membership against mount roots, including cgroup namespaces."""
    def unescape(text):
        return re.sub(r'\\([0-7]{3})', lambda match: chr(int(match[1], 8)), text)
    try:
        groups = Path('/proc/self/cgroup').read_text().splitlines()
        mounts = Path('/proc/self/mountinfo').read_text().splitlines()
    except OSError:
        return None
    candidates = []
    for group in groups:
        try:
            _, controllers, relative = group.split(':', 2)
            relative = Path(relative)
            if not relative.is_absolute() or '..' in relative.parts:
                continue
            for line in mounts:
                before, after = line.split(' - ', 1)
                fs, _, options = after.split(' ', 2)
                unified = fs == 'cgroup2' and not controllers
                if not unified and not (fs == 'cgroup' and 'memory' in controllers.split(',') and 'memory' in options.split(',')):
                    continue
                fields = before.split()
                root, mount = Path(unescape(fields[3])), Path(unescape(fields[4]))
                if not mount.is_absolute() or not root.is_absolute() or '..' in mount.parts or '..' in root.parts:
                    continue
                try:
                    suffix = relative.relative_to(root)
                except ValueError:
                    # Namespaced membership '/' denotes the mounted subtree.
                    if relative != Path('/'):
                        continue
                    suffix = Path('.')
                directory = mount / suffix
                if directory.is_dir():
                    candidates.append((len(root.parts), MemoryController(directory, mount, unified)))
        except (ValueError, IndexError):
            continue
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def cgroup_sample(controller):
    """Visible ancestor limits and usage constrain the leaf's remaining space."""
    if controller is None:
        return None
    unified = controller.unified
    current_name = 'memory.current' if unified else 'memory.usage_in_bytes'
    limit_name = 'memory.max' if unified else 'memory.limit_in_bytes'
    limits = []
    for directory in controller.ancestors():
        limit = _integer(directory / limit_name)
        if limit is None or not unified and limit >= 1 << 60:
            continue
        limits.append((limit, _integer(directory / current_name), str(directory)))
    smallest = min(limits, key=lambda item: item[0]) if limits else None
    constraints = [(limit, max(0, limit - used)) for limit, used, _ in limits if used is not None]
    events = {}
    if unified:
        try:
            events = {key: int(value) for key, value in (line.split() for line in
                (controller.directory / 'memory.events').read_text().splitlines())}
        except (OSError, ValueError):
            pass
    return dict(scope='shared_cgroup', path=str(controller.directory),
        current_bytes=_integer(controller.directory / current_name),
        limit_bytes=smallest[0] if smallest else None,
        limit_path=smallest[2] if smallest else None,
        limit_scope_current_bytes=smallest[1] if smallest else None,
        available_bytes=min((available for _, available in constraints), default=None),
        events=events, constraints=constraints)


@dataclass(frozen=True)
class MemoryResourcesSnapshot:
    budget_bytes: int
    budget_source: str
    capacity_bytes: int | None
    capacity_source: str | None
    system_total_bytes: int | None
    system_available_bytes: int | None
    cgroup: dict | None
    constraints: tuple

    def pressure_ratio(self):
        return max((1 - available / max(1, capacity) for capacity, available in self.constraints), default=0.)

    def reclaim_bytes(self, target_ratio):
        return max((max(0, capacity * (1 - target_ratio) - available)
                    for capacity, available in self.constraints), default=0.)

    def metadata(self):
        return dict(budget_source=self.budget_source, capacity_bytes=self.capacity_bytes,
            capacity_source=self.capacity_source, system_total_bytes=self.system_total_bytes,
            system_available_bytes=self.system_available_bytes, shared_pressure_ratio=self.pressure_ratio(),
            cgroup_available_bytes=self.cgroup['available_bytes'] if self.cgroup else None)


class MemoryResources:
    def __init__(self, configured_bytes):
        self.configured_bytes = configured_bytes
        self.controller = memory_controller()

    def sample(self):
        total, available = system_memory()
        cgroup = cgroup_sample(self.controller)
        capacities = [(total, 'physical')] if total is not None else []
        if cgroup and cgroup['limit_bytes'] is not None:
            capacities.append((cgroup['limit_bytes'], 'cgroup'))
        capacity, source = min(capacities, key=lambda item: item[0]) if capacities else (None, None)
        budget = self.configured_bytes or (max(1, capacity * 4 // 5) if capacity is not None else FALLBACK_BUDGET)
        constraints = [(total, min(total, available))] if total is not None and available is not None else []
        if cgroup:
            constraints.extend(cgroup.pop('constraints'))
        return MemoryResourcesSnapshot(budget, 'explicit' if self.configured_bytes else source or 'fallback',
            capacity, source, total, available, cgroup, tuple(constraints))
