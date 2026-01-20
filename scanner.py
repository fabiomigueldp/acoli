#!/usr/bin/env python3
"""
scanner.py - Project Snapshot Tool for LLM Context

Captures a complete snapshot of a directory structure and file contents,
optimized for feeding into Large Language Models.

Usage: python scanner.py [directory]
       If no directory specified, uses current working directory.
"""

import os
import sys
import subprocess
from pathlib import Path
from typing import Set, List, Tuple, Optional

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                              CONFIGURATION                                    ║
# ╠══════════════════════════════════════════════════════════════════════════════╣
# ║  Customize these settings according to your project needs                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# ─────────────────────────────────────────────────────────────────────────────────
# DIRECTORIES TO IGNORE (will not appear in tree or have files scanned)
# ─────────────────────────────────────────────────────────────────────────────────
IGNORE_DIRS: Set[str] = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".nox",
    ".eggs",
    "*.egg-info",
    ".venv",
    "venv",
    "env",
    ".env",
    "site-packages",
    ".ipynb_checkpoints",
    ".git",
    ".svn",
    ".hg",
    ".bzr",
    ".vscode",
    ".idea",
    ".vs",
    ".fleet",
    ".zed",
    "node_modules",
    ".npm",
    ".yarn",
    ".pnpm-store",
    "bower_components",
    "dist",
    "build",
    "out",
    "target",
    "_build",
    ".build",
    "public/build",
    ".next",
    ".nuxt",
    ".output",
    ".svelte-kit",
    ".parcel-cache",
    ".turbo",
    "coverage",
    "htmlcov",
    ".coverage",
    ".nyc_output",
    ".hypothesis",
    ".cache",
    ".sass-cache",
    ".eslintcache",
    ".stylelintcache",
    "tmp",
    "temp",
    ".temp",
    ".tmp",
    "__MACOSX",
    ".Trash-*",
}

# ─────────────────────────────────────────────────────────────────────────────────
# FILES TO IGNORE (exact name match - will not appear in tree)
# ─────────────────────────────────────────────────────────────────────────────────
IGNORE_FILES: Set[str] = {
    ".DS_Store",
    ".AppleDouble",
    ".LSOverride",
    "Thumbs.db",
    "ehthumbs.db",
    "Desktop.ini",
    ".directory",
    ".gitignore",
    ".gitattributes",
    ".gitmodules",
    ".gitkeep",
    ".keep",
    ".hgignore",
    ".svnignore",
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "poetry.lock",
    "Pipfile.lock",
    "composer.lock",
    "Gemfile.lock",
    "Cargo.lock",
    "flake.lock",
    "bun.lockb",
    ".editorconfig",
    ".prettierrc",
    ".prettierrc.json",
    ".prettierrc.yml",
    ".prettierrc.yaml",
    ".prettierrc.js",
    ".prettierignore",
    ".eslintrc",
    ".eslintrc.json",
    ".eslintrc.js",
    ".eslintrc.yml",
    ".eslintrc.yaml",
    ".eslintignore",
    ".stylelintrc",
    ".stylelintrc.json",
    ".stylelintignore",
    ".browserslistrc",
    "browserslist",
    ".nvmrc",
    ".npmrc",
    ".yarnrc",
    ".yarnrc.yml",
    ".node-version",
    ".python-version",
    ".ruby-version",
    ".tool-versions",
    ".travis.yml",
    "azure-pipelines.yml",
    "netlify.toml",
    "vercel.json",
    "render.yaml",
    "fly.toml",
    "railway.json",
    "Procfile",
    "pytest.ini",
    "setup.cfg",
    "tox.ini",
    "noxfile.py",
    ".coveragerc",
    "jest.config.js",
    "jest.config.ts",
    "vitest.config.ts",
    "playwright.config.ts",
    "cypress.json",
    ".npmignore",
    "CODEOWNERS",
    ".mailmap",
    "LICENSE",
    "LICENSE.md",
    "LICENSE.txt",
    "CHANGELOG.md",
    "CHANGELOG",
    "CHANGES.md",
    "CHANGES",
    "AUTHORS",
    "CONTRIBUTORS",
}

# ─────────────────────────────────────────────────────────────────────────────────
# FILE EXTENSIONS TO IGNORE (will not appear in tree)
# ─────────────────────────────────────────────────────────────────────────────────
IGNORE_EXTENSIONS: Set[str] = {
    ".pyc",
    ".pyo",
    ".pyd",
    ".o",
    ".obj",
    ".so",
    ".dll",
    ".dylib",
    ".a",
    ".lib",
    ".class",
    ".jar",
    ".war",
    ".ear",
    ".log",
    ".bak",
    ".swp",
    ".swo",
    ".swn",
    ".tmp",
    "~",
}

# ─────────────────────────────────────────────────────────────────────────────────
# FILES TO ALWAYS INCLUDE (even if they match ignore patterns)
# ─────────────────────────────────────────────────────────────────────────────────
ALWAYS_INCLUDE: Set[str] = {
    ".env",
    ".env.local",
    ".env.example",
    ".env.sample",
    ".env.template",
    ".env.development",
    ".env.production",
    ".env.staging",
    ".env.test",
}

# ─────────────────────────────────────────────────────────────────────────────────
# BINARY EXTENSIONS (appear in tree with [binary] marker, content NOT captured)
# ─────────────────────────────────────────────────────────────────────────────────
BINARY_EXTENSIONS: Set[str] = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".bmp",
    ".ico",
    ".icns",
    ".svg",
    ".webp",
    ".avif",
    ".heic",
    ".heif",
    ".tiff",
    ".tif",
    ".psd",
    ".ai",
    ".eps",
    ".raw",
    ".cr2",
    ".nef",
    ".orf",
    ".sr2",
    ".mp3",
    ".wav",
    ".ogg",
    ".flac",
    ".aac",
    ".wma",
    ".m4a",
    ".opus",
    ".aiff",
    ".mid",
    ".midi",
    ".mp4",
    ".avi",
    ".mov",
    ".mkv",
    ".webm",
    ".wmv",
    ".flv",
    ".m4v",
    ".mpeg",
    ".mpg",
    ".3gp",
    ".3g2",
    ".zip",
    ".tar",
    ".gz",
    ".bz2",
    ".xz",
    ".lz",
    ".7z",
    ".rar",
    ".tgz",
    ".tbz2",
    ".lzma",
    ".zst",
    ".lz4",
    ".cab",
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".odt",
    ".ods",
    ".odp",
    ".pages",
    ".numbers",
    ".key",
    ".epub",
    ".mobi",
    ".ttf",
    ".otf",
    ".woff",
    ".woff2",
    ".eot",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".mdb",
    ".accdb",
    ".pickle",
    ".pkl",
    ".npy",
    ".npz",
    ".h5",
    ".hdf5",
    ".hdf",
    ".mat",
    ".parquet",
    ".feather",
    ".arrow",
    ".tfrecord",
    ".onnx",
    ".pt",
    ".pth",
    ".safetensors",
    ".gguf",
    ".ggml",
    ".bin",
    ".model",
    ".weights",
    ".exe",
    ".msi",
    ".app",
    ".dmg",
    ".deb",
    ".rpm",
    ".snap",
    ".flatpak",
    ".apk",
    ".ipa",
    ".aab",
    ".iso",
    ".img",
    ".pem",
    ".crt",
    ".cer",
    ".key",
    ".p12",
    ".pfx",
    ".wasm",
    ".blend",
    ".unity",
    ".unitypackage",
    ".fbx",
    ".obj",
    ".stl",
    ".gltf",
    ".glb",
}

# ─────────────────────────────────────────────────────────────────────────────────
# SIZE LIMITS
# ─────────────────────────────────────────────────────────────────────────────────

# Maximum file size to include content (in bytes)
# Files larger than this appear in tree with [large] marker
MAX_FILE_SIZE: int = 102400

# Files smaller than this are considered empty
MIN_FILE_SIZE: int = 1

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                           END OF CONFIGURATION                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝


class Scanner:
    """Directory scanner that creates snapshots for LLM context."""
    
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.self_name = Path(__file__).name
        self.tree_lines: List[str] = []
        self.files_to_include: List[Tuple[Path, str]] = []  # (filepath, relative_path)
        self.stats = {"dirs": 0, "files": 0, "included": 0, "binary": 0, "large": 0, "empty": 0}
    
    def should_ignore_dir(self, name: str) -> bool:
        """Check if directory should be completely ignored."""
        # Check exact match
        if name in IGNORE_DIRS:
            return True
        # Check pattern match (e.g., *.egg-info)
        for pattern in IGNORE_DIRS:
            if "*" in pattern:
                if pattern.startswith("*") and name.endswith(pattern[1:]):
                    return True
                elif pattern.endswith("*") and name.startswith(pattern[:-1]):
                    return True
        # Ignore hidden dirs except those in ALWAYS_INCLUDE
        if name.startswith(".") and name not in ALWAYS_INCLUDE:
            return True
        return False
    
    def should_ignore_file(self, name: str, ext: str) -> bool:
        """Check if file should be completely ignored (not even in tree)."""
        # Always include priority files
        if name in ALWAYS_INCLUDE:
            return False
        # Ignore self
        if name == self.self_name:
            return True
        # Check exact name match
        if name in IGNORE_FILES:
            return True
        # Check extension
        if ext in IGNORE_EXTENSIONS:
            return True
        return False
    
    def get_file_status(self, filepath: Path) -> Tuple[bool, str]:
        """
        Determine if file content should be included.
        Returns: (should_include, status_marker)
        
        Status markers:
        - "" : include content
        - "binary" : binary file
        - "large" : file too large
        - "empty" : empty file
        """
        ext = filepath.suffix.lower()
        
        # Check if binary
        if ext in BINARY_EXTENSIONS:
            return False, "binary"
        
        # Check size
        try:
            size = filepath.stat().st_size
        except OSError:
            return False, "error"
        
        if size < MIN_FILE_SIZE:
            return False, "empty"
        
        if size > MAX_FILE_SIZE:
            return False, "large"
        
        return True, ""
    
    def scan(self) -> None:
        """Scan the directory tree."""
        self.tree_lines = [f"{self.root.name}/"]
        self._scan_recursive(self.root, "")
    
    def _scan_recursive(self, current: Path, prefix: str) -> None:
        """Recursively scan directory."""
        try:
            entries = sorted(
                current.iterdir(),
                key=lambda p: (p.is_file(), p.name.lower())
            )
        except PermissionError:
            return
        
        # Separate and filter
        dirs = []
        files = []
        
        for entry in entries:
            if entry.is_dir():
                if not self.should_ignore_dir(entry.name):
                    dirs.append(entry)
                    self.stats["dirs"] += 1
            elif entry.is_file():
                ext = entry.suffix.lower()
                if not self.should_ignore_file(entry.name, ext):
                    files.append(entry)
                    self.stats["files"] += 1
        
        # Process all entries
        all_entries = dirs + files
        total = len(all_entries)
        
        for i, entry in enumerate(all_entries):
            is_last = (i == total - 1)
            connector = "└── " if is_last else "├── "
            child_prefix = prefix + ("    " if is_last else "│   ")
            
            if entry.is_dir():
                self.tree_lines.append(f"{prefix}{connector}{entry.name}/")
                self._scan_recursive(entry, child_prefix)
            else:
                should_include, status = self.get_file_status(entry)
                
                # Add to tree
                marker = f" [{status}]" if status else ""
                self.tree_lines.append(f"{prefix}{connector}{entry.name}{marker}")
                
                # Track stats
                if status == "binary":
                    self.stats["binary"] += 1
                elif status == "large":
                    self.stats["large"] += 1
                elif status == "empty":
                    self.stats["empty"] += 1
                
                # Add to files list if content should be included
                if should_include:
                    rel_path = str(entry.relative_to(self.root))
                    self.files_to_include.append((entry, rel_path))
                    self.stats["included"] += 1
    
    def read_file(self, filepath: Path) -> str:
        """Read file content with encoding fallback."""
        encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "ascii"]
        
        for encoding in encodings:
            try:
                return filepath.read_text(encoding=encoding)
            except UnicodeDecodeError:
                continue
            except Exception as e:
                return f"[Error reading file: {e}]"
        
        return "[Unable to decode file]"
    
    def get_lang_hint(self, filepath: Path) -> str:
        """Get markdown code block language hint."""
        ext_to_lang = {
            # Python
            ".py": "python", ".pyw": "python", ".pyx": "python", ".pxd": "python",
            ".pyi": "python",
            # JavaScript / TypeScript
            ".js": "javascript", ".mjs": "javascript", ".cjs": "javascript",
            ".ts": "typescript", ".mts": "typescript", ".cts": "typescript",
            ".jsx": "jsx", ".tsx": "tsx",
            # Web
            ".html": "html", ".htm": "html", ".xhtml": "html",
            ".css": "css", ".scss": "scss", ".sass": "sass", ".less": "less",
            ".vue": "vue", ".svelte": "svelte", ".astro": "astro",
            # Data
            ".json": "json", ".jsonc": "jsonc", ".json5": "json5",
            ".xml": "xml", ".xsl": "xml", ".xslt": "xml",
            ".yaml": "yaml", ".yml": "yaml",
            ".toml": "toml",
            ".csv": "csv",
            ".ini": "ini", ".cfg": "ini", ".conf": "ini",
            # Shell
            ".sh": "bash", ".bash": "bash", ".zsh": "zsh", ".fish": "fish",
            ".ps1": "powershell", ".psm1": "powershell", ".psd1": "powershell",
            ".bat": "batch", ".cmd": "batch",
            # Documentation
            ".md": "markdown", ".markdown": "markdown", ".mdx": "mdx",
            ".rst": "rst", ".txt": "text",
            # Systems
            ".c": "c", ".h": "c",
            ".cpp": "cpp", ".cc": "cpp", ".cxx": "cpp",
            ".hpp": "cpp", ".hxx": "cpp", ".hh": "cpp",
            ".cs": "csharp",
            ".go": "go",
            ".rs": "rust",
            ".java": "java",
            ".kt": "kotlin", ".kts": "kotlin",
            ".scala": "scala", ".sc": "scala",
            ".swift": "swift",
            ".m": "objectivec", ".mm": "objectivec",
            # Scripting
            ".rb": "ruby", ".rake": "ruby", ".gemspec": "ruby",
            ".php": "php",
            ".pl": "perl", ".pm": "perl",
            ".lua": "lua",
            ".r": "r", ".R": "r",
            # Functional
            ".ex": "elixir", ".exs": "elixir",
            ".erl": "erlang", ".hrl": "erlang",
            ".clj": "clojure", ".cljs": "clojure", ".cljc": "clojure", ".edn": "clojure",
            ".hs": "haskell", ".lhs": "haskell",
            ".ml": "ocaml", ".mli": "ocaml",
            ".fs": "fsharp", ".fsx": "fsharp", ".fsi": "fsharp",
            ".elm": "elm",
            ".nim": "nim",
            ".zig": "zig",
            # Query
            ".sql": "sql", ".mysql": "sql", ".pgsql": "sql",
            ".graphql": "graphql", ".gql": "graphql",
            # Config / DevOps
            ".dockerfile": "dockerfile",
            ".tf": "terraform", ".tfvars": "terraform",
            ".hcl": "hcl",
            ".nix": "nix",
            ".dhall": "dhall",
            # Other
            ".proto": "protobuf",
            ".prisma": "prisma",
            ".vim": "vim", ".vimrc": "vim",
            ".tex": "latex", ".latex": "latex",
            ".env": "bash",
            ".gitignore": "gitignore",
            ".editorconfig": "editorconfig",
            ".htaccess": "apacheconf",
        }
        
        name_to_lang = {
            "Dockerfile": "dockerfile",
            "Containerfile": "dockerfile",
            "Makefile": "makefile",
            "GNUmakefile": "makefile",
            "makefile": "makefile",
            "Justfile": "just",
            "justfile": "just",
            "Jenkinsfile": "groovy",
            "Vagrantfile": "ruby",
            "Gemfile": "ruby",
            "Rakefile": "ruby",
            "Guardfile": "ruby",
            "Podfile": "ruby",
            "Brewfile": "ruby",
            "CMakeLists.txt": "cmake",
            "meson.build": "meson",
            "BUILD": "python",
            "BUILD.bazel": "python",
            "WORKSPACE": "python",
            "requirements.txt": "text",
            "constraints.txt": "text",
            "pyproject.toml": "toml",
            "setup.py": "python",
            "setup.cfg": "ini",
            "Cargo.toml": "toml",
            "go.mod": "go",
            "go.sum": "text",
            "package.json": "json",
            "tsconfig.json": "jsonc",
            "jsconfig.json": "jsonc",
            "deno.json": "jsonc",
            "composer.json": "json",
            ".babelrc": "json",
            ".swcrc": "json",
        }
        
        name = filepath.name
        if name in name_to_lang:
            return name_to_lang[name]
        
        ext = filepath.suffix.lower()
        return ext_to_lang.get(ext, "")
    
    def generate_snapshot(self) -> str:
        """Generate the complete snapshot string."""
        parts: List[str] = []
        
        # Header
        parts.append(f"# {self.root.name}")
        parts.append("")
        
        # Tree structure
        parts.append("## Structure")
        parts.append("")
        parts.append("```")
        parts.extend(self.tree_lines)
        parts.append("```")
        parts.append("")
        
        # File contents
        if self.files_to_include:
            parts.append("## Files")
            parts.append("")
            
            for filepath, rel_path in self.files_to_include:
                content = self.read_file(filepath).rstrip()
                lang = self.get_lang_hint(filepath)
                
                parts.append(f"### {rel_path}")
                parts.append(f"```{lang}")
                parts.append(content)
                parts.append("```")
                parts.append("")
        
        return "\n".join(parts)


def copy_to_clipboard(text: str) -> bool:
    """Copy text to clipboard. Cross-platform support."""
    try:
        if sys.platform == "darwin":
            # macOS
            process = subprocess.Popen(
                ["pbcopy"],
                stdin=subprocess.PIPE,
                stderr=subprocess.DEVNULL
            )
            process.communicate(text.encode("utf-8"))
            return process.returncode == 0
        
        elif sys.platform == "win32":
            # Windows - use clip.exe with UTF-16LE encoding (native Windows Unicode)
            process = subprocess.Popen(
                ["clip.exe"],
                stdin=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
            )
            # clip.exe expects UTF-16LE encoding for proper Unicode support
            process.communicate(text.encode("utf-16le"))
            return process.returncode == 0
        
        else:
            # Linux - try xclip, then xsel, then wl-copy (Wayland)
            clipboard_commands = [
                ["xclip", "-selection", "clipboard"],
                ["xsel", "--clipboard", "--input"],
                ["wl-copy"],
            ]
            
            for cmd in clipboard_commands:
                try:
                    process = subprocess.Popen(
                        cmd,
                        stdin=subprocess.PIPE,
                        stderr=subprocess.DEVNULL
                    )
                    process.communicate(text.encode("utf-8"))
                    if process.returncode == 0:
                        return True
                except FileNotFoundError:
                    continue
            
            return False
            
    except Exception:
        return False


def format_size(size: int) -> str:
    """Format byte size to human readable."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{size} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def estimate_tokens(text: str) -> int:
    """Rough estimate of tokens (chars / 4 is a common approximation)."""
    return len(text) // 4


def main():
    """Main entry point."""
    # Determine target directory
    if len(sys.argv) > 1:
        target = Path(sys.argv[1])
        if not target.exists():
            print(f"❌ Error: Path does not exist: {target}")
            sys.exit(1)
        if not target.is_dir():
            print(f"❌ Error: Path is not a directory: {target}")
            sys.exit(1)
        root = target.resolve()
    else:
        root = Path.cwd()
    
    # Banner
    print("┌─────────────────────────────────────────┐")
    print("│      📸 Project Snapshot Scanner        │")
    print("└─────────────────────────────────────────┘")
    print()
    print(f"📂 Target: {root}")
    print()
    
    # Scan
    scanner = Scanner(root)
    scanner.scan()
    
    # Generate snapshot
    snapshot = scanner.generate_snapshot()
    
    # Stats
    char_count = len(snapshot)
    line_count = snapshot.count("\n") + 1
    token_estimate = estimate_tokens(snapshot)
    
    print("📊 Statistics:")
    print(f"   ├── Directories:   {scanner.stats['dirs']}")
    print(f"   ├── Files found:   {scanner.stats['files']}")
    print(f"   ├── Files included:{scanner.stats['included']}")
    print(f"   ├── Binary files:  {scanner.stats['binary']}")
    print(f"   ├── Large files:   {scanner.stats['large']}")
    print(f"   └── Empty files:   {scanner.stats['empty']}")
    print()
    print("📄 Snapshot:")
    print(f"   ├── Lines:         {line_count:,}")
    print(f"   ├── Characters:    {char_count:,}")
    print(f"   ├── Size:          {format_size(char_count)}")
    print(f"   └── Est. tokens:   ~{token_estimate:,}")
    print()
    
    # Copy to clipboard
    if copy_to_clipboard(snapshot):
        print("✅ Snapshot copied to clipboard!")
    else:
        print("⚠️  Could not copy to clipboard.")
        print("   Saving to 'snapshot.md' instead...")
        try:
            output_file = root / "snapshot.md"
            output_file.write_text(snapshot, encoding="utf-8")
            print(f"   ✅ Saved to: {output_file}")
        except Exception as e:
            print(f"   ❌ Error saving file: {e}")
            print()
            print("─" * 50)
            print("Snapshot output:")
            print("─" * 50)
            print(snapshot)


if __name__ == "__main__":
    main()
