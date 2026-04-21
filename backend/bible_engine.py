"""
Coder's Bible — Knowledge Engine
Queries the 53,018-fragment Bible database via FTS5.
Language detection via regex fingerprints + source analysis.
"""

import re
import sqlite3
import os
from typing import Optional

# ─── Database Path ───────────────────────────────────────────
# Standalone Bible database — fully separated from Omega Brain
DB_PATH = os.environ.get(
    "BIBLE_DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "coders_bible.db")
)

# ─── Language Fingerprints ───────────────────────────────────
# Each entry: (language, [regex patterns], priority)
FINGERPRINTS = [
    # Shell / Bash
    ("Bash", [
        r"^\s*#!/bin/(ba)?sh",
        r"^\s*(sudo|apt|yum|dnf|brew|pacman|chmod|chown|chgrp|mkdir|rmdir|cp|mv|rm|ls|cat|grep|awk|sed|find|xargs|tar|curl|wget|ssh|scp|rsync|systemctl|journalctl|crontab)\b",
        r"^\s*export\s+\w+=",
        r"\|\s*(grep|awk|sed|sort|uniq|wc|head|tail|cut|tr)\b",
        r"^\s*(if|then|fi|elif|else|for|do|done|while|case|esac)\b.*;\s*$",
        r"\$\{?\w+\}?",
        r"^\s*echo\s+",
    ], 10),

    # PowerShell
    ("PowerShell", [
        r"^\s*(Get-|Set-|New-|Remove-|Invoke-|Start-|Stop-|Write-|Read-|Test-|Import-|Export-|Select-|Where-|ForEach-)\w+",
        r"\$\w+\s*=\s*",
        r"\|\s*(Select-Object|Where-Object|ForEach-Object|Sort-Object|Group-Object|Measure-Object)\b",
        r"^\s*\[Parameter\(",
        r"^\s*param\s*\(",
        r"-\w+\s+\$",
    ], 10),

    # Python
    ("Python", [
        r"^\s*def\s+\w+\s*\(.*\)\s*(->\s*\w+)?\s*:",
        r"^\s*class\s+\w+(\(.*\))?\s*:",
        r"^\s*(import|from)\s+\w+",
        r"^\s*if\s+__name__\s*==\s*['\"]__main__['\"]",
        r"^\s*@\w+",
        r"^\s*(print|len|range|enumerate|zip|map|filter|lambda)\s*\(",
        r"^\s*(try|except|finally|raise|with|as|yield|async|await)\b",
        r"self\.\w+",
    ], 9),

    # JavaScript
    ("JavaScript", [
        r"^\s*(const|let|var)\s+\w+\s*=",
        r"^\s*function\s+\w+\s*\(",
        r"=>\s*\{",
        r"\bconsole\.(log|warn|error|info)\s*\(",
        r"^\s*(module\.exports|export\s+(default|const|function|class))\b",
        r"\b(document|window|addEventListener|querySelector|fetch)\b",
        r"^\s*import\s+.*\s+from\s+['\"]",
        r"\.then\s*\(|\.catch\s*\(|async\s+function",
    ], 8),

    # TypeScript
    ("TypeScript", [
        r":\s*(string|number|boolean|void|any|never|unknown|undefined)\b",
        r"^\s*interface\s+\w+",
        r"^\s*type\s+\w+\s*=",
        r"<\w+(\s*,\s*\w+)*>",
        r"^\s*(public|private|protected|readonly)\s+",
        r"\bas\s+(string|number|boolean|any|unknown)\b",
        r"^\s*enum\s+\w+\s*\{",
    ], 9),

    # Rust
    ("Rust", [
        r"^\s*fn\s+\w+\s*(<.*>)?\s*\(",
        r"^\s*(pub|mod|use|crate|extern|impl|trait|struct|enum)\b",
        r"\b(let\s+mut|&mut|&self|Self|Option<|Result<|Vec<|Box<|Rc<|Arc<)\b",
        r"^\s*#\[(derive|cfg|test|allow|warn|deny)\b",
        r"\b(unwrap|expect|match|Some|None|Ok|Err)\b",
        r"^\s*macro_rules!\s*\w+",
        r"::\s*new\s*\(",
    ], 9),

    # Go
    ("Go", [
        r"^\s*func\s+(\(\w+\s+\*?\w+\)\s+)?\w+\s*\(",
        r"^\s*package\s+\w+",
        r"^\s*import\s+\(",
        r"\b(fmt|os|io|net|http|json|sync|context|errors)\.\w+",
        r":=\s*",
        r"^\s*type\s+\w+\s+(struct|interface)\s*\{",
        r"\bgo\s+func\b|\bdefer\b|\bgoroutine\b",
        r"\bchan\s+\w+|\bselect\s*\{",
    ], 9),

    # Ruby
    ("Ruby", [
        r"^\s*def\s+\w+",
        r"^\s*class\s+\w+(\s*<\s*\w+)?",
        r"^\s*module\s+\w+",
        r"\b(puts|print|require|require_relative|attr_accessor|attr_reader|attr_writer)\b",
        r"^\s*end\s*$",
        r"\bdo\s*\|.*\|",
        r"\.(each|map|select|reject|reduce|inject|collect|detect|find)\b",
        r"^\s*gem\s+['\"]",
    ], 8),

    # PHP
    ("PHP", [
        r"<\?php",
        r"\$\w+\s*=",
        r"^\s*(function|class|interface|trait|namespace|use)\b",
        r"\b(echo|print_r|var_dump|isset|empty|unset|array|foreach|require_once|include)\b",
        r"->\w+\s*\(",
        r"::\w+\s*\(",
    ], 8),

    # Java
    ("Java", [
        r"^\s*(public|private|protected)\s+(static\s+)?(void|int|String|boolean|double|float|long|char)\s+\w+\s*\(",
        r"^\s*class\s+\w+(\s+extends\s+\w+)?(\s+implements\s+\w+)?\s*\{",
        r"^\s*import\s+java\.",
        r"\b(System\.out\.println|System\.err|new\s+\w+\()\b",
        r"^\s*@(Override|Test|Autowired|Component|Service|Repository|Controller)\b",
        r"\b(ArrayList|HashMap|LinkedList|TreeMap|StringBuilder)\b",
    ], 8),

    # SQL
    ("SQL", [
        r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|GRANT|REVOKE)\b",
        r"\b(FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|OFFSET)\b",
        r"\b(TABLE|INDEX|VIEW|TRIGGER|PROCEDURE|FUNCTION|DATABASE|SCHEMA)\b",
        r"\b(INT|VARCHAR|TEXT|BOOLEAN|TIMESTAMP|SERIAL|PRIMARY\s+KEY|FOREIGN\s+KEY|NOT\s+NULL)\b",
    ], 7),

    # Docker
    ("Docker", [
        r"^\s*(FROM|RUN|CMD|ENTRYPOINT|COPY|ADD|EXPOSE|ENV|ARG|WORKDIR|VOLUME|USER|LABEL|HEALTHCHECK)\b",
        r"^\s*docker\s+(build|run|exec|compose|pull|push|images|ps|stop|rm|logs|inspect)\b",
    ], 8),

    # Kubernetes
    ("Kubernetes", [
        r"^\s*apiVersion:\s*",
        r"^\s*kind:\s*(Pod|Deployment|Service|ConfigMap|Secret|Ingress|DaemonSet|StatefulSet)\b",
        r"^\s*kubectl\s+(get|apply|describe|delete|logs|exec|port-forward|scale)\b",
        r"^\s*metadata:\s*$",
        r"^\s*spec:\s*$",
    ], 8),

    # Nginx
    ("Nginx", [
        r"^\s*(server|location|upstream|http|events)\s*\{",
        r"\b(proxy_pass|listen|server_name|root|index|try_files|fastcgi_pass|ssl_certificate)\b",
        r"^\s*worker_processes\b",
        r"^\s*include\s+/etc/nginx/",
    ], 8),

    # systemd
    ("systemd", [
        r"^\s*\[(Unit|Service|Install|Timer|Socket|Mount|Path)\]",
        r"^\s*(ExecStart|ExecStop|ExecReload|Restart|WantedBy|After|Before|Requires|Description)\s*=",
        r"^\s*systemctl\s+(start|stop|restart|enable|disable|status|daemon-reload)\b",
    ], 8),

    # Git
    ("Git", [
        r"^\s*git\s+(init|clone|add|commit|push|pull|fetch|merge|rebase|checkout|branch|log|diff|stash|reset|tag|remote)\b",
        r"^\s*\.gitignore\b",
    ], 7),

    # Ansible
    ("Ansible", [
        r"^\s*-\s*(name|hosts|tasks|roles|vars|handlers|become|gather_facts):",
        r"\b(ansible|playbook|inventory|galaxy|vault)\b",
        r"^\s*\w+\.\w+\.\w+:",  # collection.module.name pattern
    ], 7),

    # YAML/Config
    ("YAML", [
        r"^\s*\w+:\s*$",
        r"^\s*-\s+\w+:",
        r"^\s*---\s*$",
    ], 3),

    # JSON
    ("JSON", [
        r'^\s*\{[\s\n]*"\w+"',
        r'^\s*\[[\s\n]*\{',
    ], 3),

    # CSS
    ("CSS", [
        r"^\s*[\.\#\w\[\*:]+\s*\{",
        r"\b(color|background|margin|padding|display|flex|grid|position|border|font|transition|animation|transform)\s*:",
        r"^\s*@(media|keyframes|import|font-face)\b",
    ], 6),

    # HTML
    ("HTML", [
        r"^\s*<(!DOCTYPE|html|head|body|div|span|p|a|img|form|input|button|script|style|link|meta)\b",
        r"<\/\w+>",
    ], 5),
]

# ─── Domain Color Map ────────────────────────────────────────
DOMAIN_COLORS = {
    "Python": "#3776AB",
    "JavaScript": "#F7DF1E",
    "TypeScript": "#3178C6",
    "Rust": "#DEA584",
    "Go": "#00ADD8",
    "Ruby": "#CC342D",
    "PHP": "#777BB4",
    "Java": "#ED8B00",
    "Bash": "#4EAA25",
    "PowerShell": "#012456",
    "SQL": "#E38C00",
    "Docker": "#2496ED",
    "Kubernetes": "#326CE5",
    "Nginx": "#009639",
    "systemd": "#6C7A89",
    "Git": "#F05032",
    "Ansible": "#EE0000",
    "CSS": "#1572B6",
    "HTML": "#E34F26",
    "YAML": "#CB171E",
    "JSON": "#292929",
    "Terraform": "#7B42BC",
}

# ─── Language-to-Source Routing ───────────────────────────────
# Maps detected language to SQL LIKE patterns for source column
LANGUAGE_SOURCE_MAP = {
    "Python":     ["%python%", "%docs.python.org%"],
    "JavaScript": ["%nodejs%", "%npmjs%", "%node.js%"],
    "TypeScript":  ["%typescript%", "%typescriptlang%"],
    "Rust":       ["%rust%", "%doc.rust-lang%", "%docs.rs%"],
    "Go":         ["%golang%", "%go.dev%", "%pkg.go.dev%"],
    "Ruby":       ["%ruby%"],
    "PHP":        ["%php%"],
    "Java":       ["%java%"],
    "Bash":       ["%bash%", "%gnu.org/software/bash%", "%help:%", "%tldp.org%"],
    "PowerShell": ["%powershell%", "%microsoft.com/powershell%"],
    "SQL":        ["%mysql%", "%mariadb%", "%postgresql%", "%postgres%", "%sqlite%"],
    "Docker":     ["%docker%"],
    "Kubernetes": ["%kubernetes%", "%k8s%"],
    "Nginx":      ["%nginx%"],
    "systemd":    ["%systemd%", "%freedesktop.org%"],
    "Git":        ["%git-scm%", "%git/%"],
    "Ansible":    ["%ansible%"],
    "Terraform":  ["%terraform%"],
    "Linux":      ["%linux%", "%man7.org%", "%sourceware.org%"],
    "CSS":        ["%css%", "%w3.org%"],
    "HTML":       ["%html%", "%w3.org%"],
}

# ─── Syntax Noise Filter (all 20 domains) ────────────────────
# These are language keywords, NOT searchable concepts.
# FTS5 matching "def" in "--output-def" or "i-def" is pure noise.
SYNTAX_NOISE = {
    # Universal
    'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
    'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
    'would', 'could', 'should', 'may', 'might', 'must', 'shall',
    'not', 'and', 'or', 'but', 'so', 'yet', 'nor',
    'if', 'else', 'elif', 'then', 'fi', 'end', 'done',
    'for', 'while', 'in', 'of', 'to', 'from', 'with', 'as',
    'true', 'false', 'null', 'none', 'nil', 'void', 'undefined',
    # Python
    'def', 'class', 'import', 'return', 'pass', 'break', 'continue',
    'try', 'except', 'finally', 'raise', 'with', 'yield', 'async',
    'await', 'lambda', 'global', 'nonlocal', 'assert', 'del',
    'self', 'cls', 'print', 'len', 'range', 'type', 'list', 'dict',
    'set', 'tuple', 'int', 'str', 'bool', 'float', 'bytes',
    # JavaScript / TypeScript
    'var', 'let', 'const', 'function', 'new', 'this', 'super',
    'typeof', 'instanceof', 'switch', 'case', 'default', 'throw',
    'catch', 'export', 'extends', 'implements', 'interface',
    'enum', 'abstract', 'static', 'readonly', 'public', 'private',
    'protected', 'string', 'number', 'boolean', 'any', 'never',
    'unknown', 'object', 'symbol', 'console', 'log', 'module',
    # Rust
    'fn', 'pub', 'mod', 'use', 'crate', 'extern', 'impl', 'trait',
    'struct', 'match', 'mut', 'ref', 'move', 'unsafe', 'where',
    'loop', 'Some', 'None', 'Ok', 'Err', 'unwrap', 'expect',
    # Go
    'func', 'package', 'chan', 'select', 'defer', 'go', 'map',
    'make', 'append', 'fmt', 'err', 'error', 'nil',
    # Ruby
    'puts', 'require', 'attr', 'begin', 'rescue', 'ensure',
    'unless', 'until', 'when', 'defined',
    # PHP
    'echo', 'isset', 'empty', 'unset', 'array', 'foreach',
    'include', 'namespace', 'php',
    # Java
    'void', 'main', 'args', 'System', 'out', 'println',
    'extends', 'implements', 'throws', 'final', 'native',
    # Shell / Bash
    'fi', 'esac', 'done', 'do', 'then', 'elif',
    'exit', 'shift', 'source', 'eval', 'exec',
    # SQL
    'select', 'insert', 'update', 'delete', 'create', 'alter',
    'drop', 'table', 'index', 'view', 'where', 'join', 'left',
    'right', 'inner', 'outer', 'group', 'order', 'having',
    'limit', 'offset', 'values', 'into', 'column',
    # Docker
    'run', 'cmd', 'copy', 'add', 'expose', 'env', 'arg',
    'workdir', 'volume', 'user', 'label', 'entrypoint',
    # Kubernetes
    'kind', 'spec', 'metadata', 'name', 'containers',
    'image', 'ports', 'replicas',
    # Nginx
    'server', 'location', 'upstream', 'listen', 'root',
    'proxy', 'http', 'events', 'worker',
    # Git
    'git', 'commit', 'push', 'pull', 'fetch', 'merge',
    'rebase', 'checkout', 'branch', 'remote', 'origin',
    # Ansible
    'hosts', 'tasks', 'roles', 'vars', 'handlers', 'become',
    'playbook', 'inventory',
    # Terraform
    'resource', 'variable', 'output', 'provider', 'data',
    'module', 'locals', 'terraform',
    # CSS / HTML
    'div', 'span', 'body', 'head', 'html', 'style', 'script',
    'color', 'background', 'margin', 'padding', 'display',
    'flex', 'grid', 'position', 'border', 'font',
}

# ─── Safety Classification ───────────────────────────────────
DANGEROUS_PATTERNS = [
    (r"\brm\s+-rf\b", "Recursive forced deletion — can destroy entire directory trees"),
    (r"\b(dd\s+if=|mkfs|fdisk|parted)\b", "Disk-level operations — can wipe drives"),
    (r"\b(chmod\s+777|chmod\s+-R\s+777)\b", "World-writable permissions — security risk"),
    (r"\b(eval|exec)\s*\(", "Dynamic code execution — injection risk if input is untrusted"),
    (r"\b(DROP\s+TABLE|DROP\s+DATABASE|TRUNCATE)\b", "Destructive database operation — data loss"),
    (r"\b(curl|wget).*\|\s*(bash|sh)\b", "Piping remote script to shell — arbitrary code execution risk"),
    (r">\s*/dev/sd[a-z]", "Writing directly to block device — will destroy filesystem"),
    (r"\b(:(){.*};)\b", "Fork bomb — will crash the system"),
]

SAFE_PATTERNS = [
    (r"^\s*(ls|pwd|whoami|hostname|date|uptime|cat|head|tail|wc|echo)\b", "Read-only / informational command"),
    (r"^\s*(git\s+(status|log|diff|branch))\b", "Read-only Git operation"),
    (r"^\s*(SELECT|SHOW|DESCRIBE|EXPLAIN)\b", "Read-only database query"),
    (r"^\s*(print|console\.log|puts|echo|fmt\.Println)\b", "Output/display operation"),
]


class BibleEngine:
    """Core engine for the Coder's Bible knowledge base."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
        self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    # ─── Language Detection ──────────────────────────────────
    def detect_language(self, snippet: str) -> dict:
        """Detect the programming language/tool from a code snippet."""
        scores = {}
        lines = snippet.strip().split("\n")

        for lang, patterns, priority in FINGERPRINTS:
            score = 0
            matched_patterns = []
            for pattern in patterns:
                for line in lines:
                    if re.search(pattern, line, re.IGNORECASE):
                        score += priority
                        matched_patterns.append(pattern)
                        break  # one match per pattern is enough
            if score > 0:
                scores[lang] = {
                    "score": score,
                    "matches": len(matched_patterns),
                    "matched_patterns": matched_patterns,
                    "total_patterns": len(patterns),
                    "confidence": min(1.0, len(matched_patterns) / max(3, len(patterns) * 0.5))
                }

        if not scores:
            return {
                "language": "Unknown",
                "confidence": 0,
                "confidence_label": "UNVERIFIED",
                "reasoning": "No structural or semantic syntax signatures were detected.",
                "context": "Agnostic",
                "color": "#666666",
                "alternatives": []
            }

        # Sort by score descending
        ranked = sorted(scores.items(), key=lambda x: x[1]["score"], reverse=True)
        primary = ranked[0]
        confidence = round(primary[1]["confidence"], 2)
        
        # Confidence label mapping
        if confidence >= 0.8:
            conf_label = "HIGH_AUTHORITY"
        elif confidence >= 0.5:
            conf_label = "VERIFIED_DOMAIN"
        else:
            conf_label = "LOW_CONFIDENCE"
            
        reasoning = f"Matched {primary[1]['matches']} deterministic syntax fingerprints (e.g., {primary[1]['matched_patterns'][0]})." if primary[1]['matches'] else "Inferred via secondary heuristics."
        
        # Context Mapping
        context_map = {
            "Python": "Standard Library / CPython",
            "Bash": "Unix Shell Environment",
            "PowerShell": "Windows Management Framework",
            "SQL": "Relational Database Engine",
            "JavaScript": "Browser Engine / Node.js Runtime",
            "Docker": "Container Daemon",
            "Kubernetes": "Cluster Control Plane",
            "Git": "Version Control System",
            "Rust": "Rustc Compilation Environment"
        }

        return {
            "language": primary[0],
            "confidence": confidence,
            "confidence_label": conf_label,
            "reasoning": reasoning,
            "context": context_map.get(primary[0], f"{primary[0]} Runtime Environment"),
            "color": DOMAIN_COLORS.get(primary[0], "#C9A84C"),
            "alternatives": [
                {"language": lang, "confidence": round(data["confidence"], 2)}
                for lang, data in ranked[1:4]
            ]
        }

    # ─── Safety Check ────────────────────────────────────────
    def check_safety(self, snippet: str) -> dict:
        """Evaluate snippet safety."""
        warnings = []
        for pattern, desc in DANGEROUS_PATTERNS:
            if re.search(pattern, snippet, re.IGNORECASE):
                warnings.append({"level": "danger", "message": desc})

        safe_notes = []
        for pattern, desc in SAFE_PATTERNS:
            if re.search(pattern, snippet, re.IGNORECASE):
                safe_notes.append(desc)

        if warnings:
            return {"level": "DESTRUCTIVE", "warnings": warnings, "safe_notes": []}
        elif safe_notes:
            return {"level": "SAFE", "warnings": [], "safe_notes": safe_notes}
        else:
            return {"level": "CAUTION", "warnings": [], "safe_notes": ["Operation lacks explicitly recognized safe boundaries. Execution requires context validation."]}

    # ─── FTS5 Search ─────────────────────────────────────────
    def _format_tier(self, raw_tier: str) -> str:
        t = (raw_tier or "").upper()
        if "A" in t or "OFFICIAL" in t:
            return "Tier A (Official)"
        elif "B" in t or "MAN" in t:
            return "Tier B (Man Pages)"
        elif "C" in t or "DERIVED" in t:
            return "Tier C (Derived/Community)"
        return "Tier C (Derived)"

    def search(self, query: str, limit: int = 20, language: str = None) -> list:
        """Search the Bible via FTS5 with optional language-domain filtering."""
        clean_query = self._sanitize_fts_query(query)
        if not clean_query:
            return []

        # If language is detected, run a two-tier search:
        #   Tier 1: domain-matched results (prioritized)
        #   Tier 2: global FTS results (fallback)
        results = []
        seen_ids = set()

        if language and language in LANGUAGE_SOURCE_MAP:
            domain_results = self._search_domain(clean_query, language, limit)
            for r in domain_results:
                if not r.get("tier"):
                    r["tier"] = self._map_source_to_tier(r.get("source", ""))
                results.append(r)
                seen_ids.add(r["id"])

        # Global FTS search for remaining slots
        remaining = limit - len(results)
        if remaining > 0:
            global_results = self._search_fts(clean_query, remaining + 10)
            for r in global_results:
                if r["id"] not in seen_ids:
                    results.append(r)
                    seen_ids.add(r["id"])
                    if len(results) >= limit:
                        break

        return results[:limit]

    def _search_domain(self, fts_query: str, language: str, limit: int) -> list:
        """Search FTS5 filtered to a specific language domain."""
        patterns = LANGUAGE_SOURCE_MAP.get(language, [])
        if not patterns:
            return []

        # Build source filter
        source_clauses = " OR ".join([f"f.source LIKE ?" for _ in patterns])
        try:
            cursor = self.conn.execute(f"""
                SELECT f.id, f.content, f.source, f.tier, rank
                FROM bible_fts fts
                JOIN fragments f ON f.rowid = fts.rowid
                WHERE bible_fts MATCH ?
                  AND ({source_clauses})
                ORDER BY rank
                LIMIT ?
            """, (fts_query, *patterns, limit))
            return [
                {
                    "id": row["id"],
                    "content": row["content"][:500],
                    "source": row["source"],
                    "tier": row["tier"] if row["tier"] else self._map_source_to_tier(row["source"]),
                    "relevance": round(abs(row["rank"]), 4)
                }
                for row in cursor
            ]
        except sqlite3.OperationalError:
            return []

    def _map_source_to_tier(self, source: str) -> str:
        """Deterministically map source URLs to evidence tiers."""
        source_lower = source.lower()
        if any(x in source_lower for x in ["docs.python.org", "developer.mozilla.org", "man7.org", "gnu.org", "postgresql.org/docs"]):
            return "Official"
        elif any(x in source_lower for x in ["man page", "man", "tldr"]):
            return "Man"
        else:
            return "Derived"

    def _search_fts(self, fts_query: str, limit: int) -> list:
        """Raw FTS5 search without domain filter."""
        try:
            cursor = self.conn.execute("""
                SELECT f.id, f.content, f.source, f.tier, rank
                FROM bible_fts fts
                JOIN fragments f ON f.rowid = fts.rowid
                WHERE bible_fts MATCH ?
                ORDER BY rank
                LIMIT ?
            """, (fts_query, limit))
            return [
                {
                    "id": row["id"],
                    "content": row["content"][:500],
                    "source": row["source"],
                    "tier": row["tier"],
                    "relevance": round(abs(row["rank"]), 4)
                }
                for row in cursor
            ]
        except sqlite3.OperationalError:
            return []

    def _fallback_search(self, query: str, limit: int) -> list:
        """LIKE-based fallback if FTS5 query fails."""
        cursor = self.conn.execute("""
            SELECT id, content, source, tier
            FROM fragments
            WHERE content LIKE ? OR source LIKE ?
            LIMIT ?
        """, (f"%{query}%", f"%{query}%", limit))
        return [
            {
                "id": row["id"],
                "content": row["content"][:500],
                "source": row["source"],
                "tier": self._format_tier(row["tier"]),
                "relevance": 0.5
            }
            for row in cursor
        ]

    def _sanitize_fts_query(self, query: str) -> str:
        """Clean a query string for FTS5 safety — strips syntax noise."""
        tokens = re.findall(r'[a-zA-Z_][\w]*', query)
        if not tokens:
            return ""
        # Filter out syntax noise across all languages
        meaningful = [t for t in tokens if t.lower() not in SYNTAX_NOISE and len(t) > 1]
        if not meaningful:
            # If ALL tokens were noise, use original (avoid empty query)
            meaningful = [t for t in tokens if len(t) > 2][:3]
        if not meaningful:
            return ""
        return " OR ".join(meaningful[:10])

    # ─── Extract Keywords from Snippet ───────────────────────
    def extract_keywords(self, snippet: str) -> list:
        """Extract searchable keywords from a code snippet.
        Strips all syntax keywords from every supported language."""
        # Remove comments and string literals
        cleaned = re.sub(r'#.*$', '', snippet, flags=re.MULTILINE)
        cleaned = re.sub(r'//.*$', '', cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
        cleaned = re.sub(r'["\'].*?["\']', '', cleaned)

        # Extract identifiers
        tokens = re.findall(r'[a-zA-Z_][\w.]*', cleaned)

        meaningful = []
        seen = set()
        for token in tokens:
            lower = token.lower()
            if lower not in SYNTAX_NOISE and lower not in seen and len(token) > 1:
                meaningful.append(token)
                seen.add(lower)

        return meaningful[:15]

    # ─── Concept Extraction ────────────────────────────────────
    CONCEPT_PATTERNS = [
        (r'\b(\w+)\s*\(.*\b\1\b', 'recursion'),
        (r'\bfor\b.*\bin\b|\bwhile\b', 'iteration loop'),
        (r'\bclass\b.*\(.*\)', 'inheritance'),
        (r'\btry\b.*\b(except|catch)\b', 'error handling exception'),
        (r'\basync\b|\bawait\b', 'asynchronous concurrency'),
        (r'\blambda\b|=>', 'lambda anonymous function'),
        (r'\bsort\b|\bsorted\b', 'sorting algorithm'),
        (r'\b(open|read|write|close)\s*\(', 'file io'),
        (r'\b(get|post|put|delete|fetch|request)\b', 'http request api'),
        (r'\b(connect|cursor|execute|query)\b', 'database query'),
        (r'\b(import|require|use|include)\b.*\b(json|yaml|xml|csv)\b', 'serialization parsing'),
        (r'\b(re\.|regex|match|search|findall)\b', 'regular expression pattern'),
        (r'\b(thread|process|pool|concurrent|parallel)\b', 'concurrency threading'),
        (r'\b(socket|listen|bind|accept)\b', 'networking socket'),
        (r'\b(encrypt|decrypt|hash|hmac|sha|md5)\b', 'cryptography security'),
        (r'\b(test|assert|expect|mock)\b', 'testing unit test'),
    ]

    def decompose_snippet(self, snippet: str) -> list:
        """Decompose the snippet into semantic chunks (Breakdown)."""
        breakdown = []
        lines = snippet.strip().split("\n")
        
        for line in lines:
            line_str = line.strip()
            if not line_str:
                continue
            
            matched_concept = None
            for pattern, concept in self.CONCEPT_PATTERNS:
                if re.search(pattern, line_str, re.IGNORECASE):
                    matched_concept = concept
                    break
                    
            if matched_concept:
                breakdown.append({"code": line_str, "concept": matched_concept.title()})
            elif len(line_str) > 3 and not line_str.startswith(("#", "//", "/*")):
                breakdown.append({"code": line_str, "concept": "Operation / Declaration"})
        
        return breakdown

    def _extract_concepts(self, snippet: str) -> list:
        """Extract high-level programming concepts from code structure."""
        concepts = []
        for pattern, concept in self.CONCEPT_PATTERNS:
            if re.search(pattern, snippet, re.IGNORECASE | re.DOTALL):
                concepts.append(concept)
        return concepts

    # ─── Full Analysis ───────────────────────────────────────
    def analyze(self, snippet: str) -> dict:
        """Full analysis pipeline: detect language, extract keywords,
        search Bible with domain-aware routing."""
        if not snippet or not snippet.strip():
            return {"error": "Empty input"}

        # 1. Detect language
        detection = self.detect_language(snippet)
        detected_lang = detection["language"]
        lang_param = detected_lang if detected_lang != "Unknown" else None

        # 2. Extract keywords (syntax noise already filtered)
        keywords = self.extract_keywords(snippet)

        # 3. Extract concepts from code structure
        concepts = self._extract_concepts(snippet)

        # 4. Build search query — keywords + concepts
        search_terms = keywords[:6] + concepts[:3]
        search_query = " OR ".join(search_terms) if search_terms else ""
        results = self.search(search_query, limit=15, language=lang_param) if search_query else []

        # 5. If few results, also search with syntax terms inside domain
        #    (e.g., "def" is noise globally but relevant in Python docs)
        if len(results) < 5 and lang_param:
            raw_tokens = re.findall(r'[a-zA-Z_][\w]*', snippet)
            syntax_in_domain = [t for t in raw_tokens if t.lower() in SYNTAX_NOISE and len(t) > 2]
            if syntax_in_domain:
                # Combine with meaningful keywords for domain-specific search
                domain_q = " OR ".join(list(set(syntax_in_domain))[:5] + keywords[:3])
                domain_results = self._search_domain(
                    self._sanitize_fts_query(domain_q) or domain_q,
                    lang_param, 10
                )
                seen = {r["id"] for r in results}
                for r in domain_results:
                    if r["id"] not in seen:
                        results.append(r)
                        seen.add(r["id"])

        # 6. Final fallback: raw identifiers > 3 chars
        if not results:
            raw_tokens = re.findall(r'[a-zA-Z_][\w]*', snippet)
            raw_meaningful = [t for t in raw_tokens if len(t) > 3][:5]
            if raw_meaningful:
                fallback_q = " OR ".join(raw_meaningful)
                results = self.search(fallback_q, limit=10, language=lang_param)

        # 7. Safety check
        safety = self.check_safety(snippet)

        # 8. Compile first line summary
        first_line = snippet.strip().split("\n")[0].strip()
        if len(first_line) > 80:
            first_line = first_line[:77] + "..."

        return {
            "input": first_line,
            "language": detection,
            "safety": safety,
            "keywords": keywords + concepts,
            "breakdown": self.decompose_snippet(snippet),
            "results": results[:15],
            "result_count": len(results),
            "total_fragments": self._get_total_count(),
        }

    def _get_total_count(self) -> int:
        """Get total fragment count."""
        try:
            row = self.conn.execute("SELECT COUNT(*) as c FROM fragments").fetchone()
            return row["c"]
        except Exception:
            return 0

    # ─── Stats ───────────────────────────────────────────────
    def get_stats(self) -> dict:
        """Get Bible database statistics."""
        try:
            total = self._get_total_count()
            domains = self.conn.execute("""
                SELECT
                    CASE
                        WHEN source LIKE '%python%' OR source LIKE '%docs.python.org%' THEN 'Python'
                        WHEN source LIKE '%nginx%' THEN 'Nginx'
                        WHEN source LIKE '%golang%' OR source LIKE '%go.dev%' OR source LIKE '%pkg.go.dev%' THEN 'Go'
                        WHEN source LIKE '%linux%' OR source LIKE '%binutils%' OR source LIKE '%man7.org%' OR source LIKE '%sourceware.org%' THEN 'Linux'
                        WHEN source LIKE '%systemd%' OR source LIKE '%freedesktop.org%' THEN 'systemd'
                        WHEN source LIKE '%nodejs%' OR source LIKE '%node.js%' OR source LIKE '%npmjs%' THEN 'Node.js'
                        WHEN source LIKE '%ruby%' THEN 'Ruby'
                        WHEN source LIKE '%rust%' OR source LIKE '%doc.rust-lang%' OR source LIKE '%docs.rs%' THEN 'Rust'
                        WHEN source LIKE '%mysql%' OR source LIKE '%mariadb%' THEN 'MySQL'
                        WHEN source LIKE '%typescript%' OR source LIKE '%typescriptlang%' THEN 'TypeScript'
                        WHEN source LIKE '%php%' THEN 'PHP'
                        WHEN source LIKE '%postgresql%' OR source LIKE '%postgres%' THEN 'PostgreSQL'
                        WHEN source LIKE '%powershell%' OR source LIKE '%microsoft.com/powershell%' THEN 'PowerShell'
                        WHEN source LIKE '%java%' AND source NOT LIKE '%javascript%' THEN 'Java'
                        WHEN source LIKE '%ansible%' THEN 'Ansible'
                        WHEN source LIKE '%docker%' THEN 'Docker'
                        WHEN source LIKE '%kubernetes%' OR source LIKE '%k8s%' THEN 'Kubernetes'
                        WHEN source LIKE '%git-scm%' OR source LIKE '%git/%' THEN 'Git'
                        WHEN source LIKE '%bash%' OR source LIKE '%gnu.org/software/bash%' THEN 'Bash'
                        ELSE 'Other'
                    END as domain,
                    COUNT(*) as count
                FROM fragments
                GROUP BY domain
                ORDER BY count DESC
            """).fetchall()

            return {
                "total_fragments": total,
                "domains": [
                    {"name": row["domain"], "count": row["count"],
                     "color": DOMAIN_COLORS.get(row["domain"], "#666")}
                    for row in domains
                ]
            }
        except Exception as e:
            return {"error": str(e), "total_fragments": 0, "domains": []}
