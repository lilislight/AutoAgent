#!/usr/bin/env python3
"""Validate the deterministic structure of a .project-memory directory."""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path


SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
H1_RE = re.compile(r"^#\s+\S", re.MULTILINE)
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")

MODULE_FIELDS = {"code_paths", "tags"}
CHANGE_FIELDS = {"modules", "tags", "related_changes", "supersedes"}
REFERENCE_FIELDS = {"related_changes", "supersedes"}


@dataclass
class Report:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def error(self, path: Path, message: str) -> None:
        self.errors.append(f"{path}: {message}")

    def warn(self, path: Path, message: str) -> None:
        self.warnings.append(f"{path}: {message}")


@dataclass
class Document:
    path: Path
    text: str
    metadata: dict[str, list[str]]
    has_frontmatter: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a repository's .project-memory directory."
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path.cwd(),
        help="Repository root. Defaults to the current directory.",
    )
    parser.add_argument(
        "--memory-dir",
        default=".project-memory",
        help="Project Memory directory relative to the repository root.",
    )
    return parser.parse_args()


def display_path(path: Path, repo: Path) -> Path:
    try:
        return path.relative_to(repo)
    except ValueError:
        return path


def unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def parse_frontmatter(path: Path, text: str, report: Report) -> tuple[dict[str, list[str]], bool]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, False

    try:
        end = next(i for i in range(1, len(lines)) if lines[i].strip() == "---")
    except StopIteration:
        report.error(path, "frontmatter starts with '---' but has no closing delimiter")
        return {}, True

    metadata: dict[str, list[str]] = {}
    current_key: str | None = None

    for line_number, raw_line in enumerate(lines[1:end], start=2):
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue

        key_match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_-]*):\s*", raw_line)
        if key_match:
            current_key = key_match.group(1)
            if current_key in metadata:
                report.error(path, f"line {line_number}: duplicate field '{current_key}'")
            metadata.setdefault(current_key, [])
            continue

        item_match = re.fullmatch(r"\s{2,}-\s+(.+?)\s*", raw_line)
        if item_match and current_key:
            value = unquote(item_match.group(1).strip())
            if not value:
                report.error(path, f"line {line_number}: empty list value")
            else:
                metadata[current_key].append(value)
            continue

        report.error(
            path,
            f"line {line_number}: only top-level list fields are supported in frontmatter",
        )

    return metadata, True


def load_document(path: Path, repo: Path, report: Report) -> Document:
    relative = display_path(path, repo)
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        report.error(relative, f"cannot read UTF-8 Markdown: {exc}")
        return Document(relative, "", {}, False)

    metadata, has_frontmatter = parse_frontmatter(relative, text, report)
    if not H1_RE.search(text):
        report.error(relative, "missing a level-one Markdown title")
    if "{{" in text or "}}" in text:
        report.error(relative, "contains an unreplaced template placeholder")
    return Document(relative, text, metadata, has_frontmatter)


def check_slug(path: Path, report: Report) -> None:
    if not SLUG_RE.fullmatch(path.stem):
        report.error(path, "file name must be lowercase kebab-case")


def check_allowed_fields(
    document: Document, allowed: set[str], report: Report
) -> None:
    for key in document.metadata:
        if key not in allowed:
            report.error(document.path, f"unsupported frontmatter field '{key}'")


def check_unique_values(document: Document, report: Report) -> None:
    for key, values in document.metadata.items():
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            report.error(
                document.path,
                f"field '{key}' contains duplicate values: {', '.join(duplicates)}",
            )


def check_tags(document: Document, report: Report) -> None:
    for tag in document.metadata.get("tags", []):
        if not SLUG_RE.fullmatch(tag):
            report.error(
                document.path,
                f"tag '{tag}' must be lowercase kebab-case",
            )


def check_code_paths(
    document: Document, repo: Path, report: Report
) -> None:
    for value in document.metadata.get("code_paths", []):
        code_path = Path(value)
        if code_path.is_absolute() or ".." in code_path.parts:
            report.error(document.path, f"code path '{value}' must stay inside the repository")
            continue
        if "#" in value or re.search(r":\d+(?:-\d+)?$", value):
            report.error(document.path, f"code path '{value}' must not include line references")
            continue
        resolved = (repo / code_path).resolve()
        try:
            resolved.relative_to(repo)
        except ValueError:
            report.error(document.path, f"code path '{value}' resolves outside the repository")
            continue
        if not resolved.exists():
            report.warn(document.path, f"code path '{value}' does not currently exist")


def check_markdown_links(document: Document, repo: Path, report: Report) -> None:
    source = repo / document.path
    for raw_target in LINK_RE.findall(document.text):
        target = raw_target.strip().strip("<>")
        if not target or target.startswith(("#", "http://", "https://", "mailto:")):
            continue
        target_path = target.split("#", 1)[0]
        if not target_path:
            continue
        resolved = (source.parent / target_path).resolve()
        try:
            resolved.relative_to(repo)
        except ValueError:
            report.error(document.path, f"Markdown link escapes repository: '{target}'")
            continue
        if not resolved.exists():
            report.error(document.path, f"broken Markdown link: '{target}'")


def check_references(
    document: Document,
    module_slugs: set[str],
    change_slugs: set[str],
    report: Report,
) -> None:
    for module in document.metadata.get("modules", []):
        if module not in module_slugs:
            report.error(document.path, f"unknown Module reference '{module}'")

    for field_name in REFERENCE_FIELDS:
        for change in document.metadata.get(field_name, []):
            if change not in change_slugs:
                report.error(
                    document.path,
                    f"unknown Change reference '{change}' in '{field_name}'",
                )
            if change == document.path.stem:
                report.error(document.path, f"'{field_name}' must not reference itself")


def check_supersedes_cycles(
    changes: dict[str, Document], report: Report
) -> None:
    graph = {
        slug: [
            target
            for target in document.metadata.get("supersedes", [])
            if target in changes
        ]
        for slug, document in changes.items()
    }
    state: dict[str, int] = {}
    stack: list[str] = []

    def visit(slug: str) -> None:
        marker = state.get(slug, 0)
        if marker == 2:
            return
        if marker == 1:
            start = stack.index(slug)
            cycle = stack[start:] + [slug]
            report.error(changes[slug].path, f"supersedes cycle: {' -> '.join(cycle)}")
            return

        state[slug] = 1
        stack.append(slug)
        for target in graph.get(slug, []):
            visit(target)
        stack.pop()
        state[slug] = 2

    for slug in graph:
        if state.get(slug, 0) == 0:
            visit(slug)


def markdown_files(directory: Path) -> Iterable[Path]:
    if not directory.is_dir():
        return []
    return sorted(path for path in directory.iterdir() if path.is_file() and path.suffix == ".md")


def validate(repo: Path, memory_dir_name: str) -> Report:
    repo = repo.resolve()
    memory_root = (repo / memory_dir_name).resolve()
    report = Report()

    try:
        memory_root.relative_to(repo)
    except ValueError:
        report.error(Path(memory_dir_name), "memory directory must stay inside the repository")
        return report

    if not memory_root.is_dir():
        report.error(Path(memory_dir_name), "Project Memory directory does not exist")
        return report

    required = [memory_root / "project.md", memory_root / "architecture.md"]
    documents: list[Document] = []
    for path in required:
        if not path.is_file():
            report.error(display_path(path, repo), "required file does not exist")
        else:
            document = load_document(path, repo, report)
            if document.has_frontmatter:
                report.error(document.path, "Project and Architecture must not use frontmatter")
            documents.append(document)

    modules_dir = memory_root / "modules"
    changes_dir = memory_root / "changes"
    if not modules_dir.is_dir():
        report.error(display_path(modules_dir, repo), "required directory does not exist")
    if not changes_dir.is_dir():
        report.error(display_path(changes_dir, repo), "required directory does not exist")

    modules: dict[str, Document] = {}
    for path in markdown_files(modules_dir):
        relative = display_path(path, repo)
        check_slug(relative, report)
        document = load_document(path, repo, report)
        if not document.has_frontmatter:
            report.warn(relative, "Module has no frontmatter for code paths or tags")
        check_allowed_fields(document, MODULE_FIELDS, report)
        check_unique_values(document, report)
        check_tags(document, report)
        check_code_paths(document, repo, report)
        modules[path.stem] = document
        documents.append(document)

    changes: dict[str, Document] = {}
    for path in markdown_files(changes_dir):
        relative = display_path(path, repo)
        check_slug(relative, report)
        document = load_document(path, repo, report)
        if not document.has_frontmatter:
            report.warn(relative, "Change has no frontmatter for Module or tag retrieval")
        check_allowed_fields(document, CHANGE_FIELDS, report)
        check_unique_values(document, report)
        check_tags(document, report)
        changes[path.stem] = document
        documents.append(document)

    module_slugs = set(modules)
    change_slugs = set(changes)
    for document in changes.values():
        check_references(document, module_slugs, change_slugs, report)
    check_supersedes_cycles(changes, report)

    for document in documents:
        check_markdown_links(document, repo, report)

    return report


def main() -> int:
    args = parse_args()
    report = validate(args.repo, args.memory_dir)

    for message in report.errors:
        print(f"ERROR: {message}")
    for message in report.warnings:
        print(f"WARNING: {message}")

    if report.errors:
        print(
            f"Project Memory validation failed with {len(report.errors)} error(s) "
            f"and {len(report.warnings)} warning(s)."
        )
        return 1

    print(f"Project Memory validation passed with {len(report.warnings)} warning(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
