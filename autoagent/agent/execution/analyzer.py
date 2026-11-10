"""Conservative static Shell analysis. Parsing never executes a script."""

from dataclasses import dataclass
import re
import shlex
from typing import Protocol


@dataclass(frozen=True)
class ShellAnalysis:
    commands: tuple[tuple[str, ...], ...] = ()
    complete: bool = False
    reason: str | None = None


class ShellCommandAnalyzer(Protocol):
    def analyze(self, script: str, shell: str) -> ShellAnalysis: ...


class TreeSitterShellAnalyzer:
    """Accept only literal commands and linear foreground command chains.

    Anything outside the allowlisted AST/lexical subset is opaque, including
    redirections, expansions, background jobs and shell-state mutation. The
    original script is executed unchanged only after whole-plan authorization.
    """

    _state_commands = frozenset({"cd", "pushd", "popd", "eval", "exec", "source", ".", "export",
        "alias", "unalias", "set", "unset", "read", "trap", "enable", "builtin", "command",
        "set-location", "set-alias", "set-variable", "invoke-expression", "iex", "sl"})

    def analyze(self, script, shell):
        try:
            from tree_sitter import Language, Parser
            if shell == "bash":
                import tree_sitter_bash as grammar
            elif shell == "pwsh":
                import tree_sitter_powershell as grammar
            else:
                return ShellAnalysis(reason="No analyzer for this Shell.")
            tree = Parser(Language(grammar.language())).parse(script.encode("utf-8"))
        except ImportError:
            return ShellAnalysis(reason="Shell parser is not installed.")
        if tree.root_node.has_error:
            return ShellAnalysis(reason="Shell syntax is incomplete or unsupported.")
        # Reject dynamic syntax even when a grammar recovers it as ordinary text.
        forbidden = "$`*?~{}()<>\\" if shell == "bash" else "$`*?{}()<>@,%"
        # Quoted literal arguments are checked separately below.
        commands = []
        allowed = ({"program", "list", "pipeline", "command"} if shell == "bash" else
            {"program", "statement_list", "pipeline", "pipeline_chain", "pipeline_chain_tail", "empty_statement", "command"})

        visited = 0
        def visit(node, depth=0):
            nonlocal visited
            visited += 1
            if visited > 2048 or depth > 64 or len(commands) > 128:
                raise ValueError("Shell analysis exceeds its structural limit.")
            if node.type == "command":
                raw = node.text.decode("utf-8")
                words = self._words(raw, shell, forbidden)
                if not words or words[0].lower() in self._state_commands:
                    raise ValueError("Command changes Shell state or uses dynamic syntax.")
                if words[0] == "printf" and "-v" in words[1:]:
                    raise ValueError("printf -v changes Shell state.")
                if shell == "pwsh" and any("=" in word for word in words):
                    raise ValueError("PowerShell assignment/parameter syntax requires whole-script approval.")
                commands.append(tuple(words))
                return
            if node.type not in allowed:
                raise ValueError("Shell syntax requires whole-script approval: " + node.type)
            for child in node.children:
                if child.is_named:
                    visit(child, depth + 1)
                elif child.text.strip() and child.text not in (b"&&", b"||", b";", b"|", b"\n", b"\r\n"):
                    raise ValueError("Unsupported Shell operator.")
        try:
            visit(tree.root_node)
            if not commands:
                raise ValueError("No static command was found.")
        except ValueError as error:
            return ShellAnalysis(reason=str(error))
        return ShellAnalysis(tuple(commands), True)

    @staticmethod
    def _words(raw, shell, forbidden):
        if shell == "bash":
            # Use the AST for command boundaries and shlex only for literal
            # quoting. Reject expansions in unquoted/double-quoted tokens.
            quote = None
            for char in raw:
                if char == "'" and quote != '"':
                    quote = None if quote == "'" else "'"
                elif char == '"' and quote != "'":
                    quote = None if quote == '"' else '"'
                elif quote != "'" and (char in forbidden or char == "=" and quote is None):
                    raise ValueError("Dynamic Shell arguments require whole-script approval.")
            return shlex.split(raw, comments=False, posix=True)
        # A narrow PowerShell tokenizer, after tree-sitter validates the script.
        # No interpolation, escapes, invocation operators, concatenated strings,
        # arrays, comments or expressions are accepted.
        pattern = r"'(?:[^']|'')*'|\"[^\"$`]*\"|[^\s'\";|&]+"
        words, position = [], 0
        for match in re.finditer(pattern, raw):
            if raw[position:match.start()].strip() or position and not raw[position:match.start()]:
                raise ValueError("Unsupported PowerShell quoting.")
            token = match.group()
            if token.startswith("'"):
                token = token[1:-1].replace("''", "'")
            elif token.startswith('"'):
                token = token[1:-1]
            elif any(char in token for char in forbidden) or token.startswith("#"):
                raise ValueError("Dynamic PowerShell arguments require whole-script approval.")
            words.append(token)
            position = match.end()
        if raw[position:].strip():
            raise ValueError("Unsupported PowerShell argument.")
        return words


__all__ = ["ShellAnalysis", "ShellCommandAnalyzer", "TreeSitterShellAnalyzer"]
