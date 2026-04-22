"""Multi-source documentation scraper for Coder's Bible."""
import requests, sqlite3, hashlib, time, re, os, sys, json
from bs4 import BeautifulSoup
from datetime import datetime

DB = os.path.join(os.path.dirname(__file__), "coders_bible.db")
HEADERS = {"User-Agent": "CodersBible/1.0 (documentation indexer)"}
DELAY = 0.3  # seconds between requests

def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        time.sleep(DELAY)
        return r.text
    except Exception as e:
        print(f"  SKIP {url}: {e}")
        return None

def chunk_text(text, max_len=1500):
    paragraphs = re.split(r'\n{2,}', text.strip())
    chunks, current = [], ""
    for p in paragraphs:
        if len(current) + len(p) > max_len and current:
            chunks.append(current.strip())
            current = p
        else:
            current += "\n\n" + p if current else p
    if current.strip():
        chunks.append(current.strip())
    return [c for c in chunks if len(c) > 50]

def insert_fragments(conn, fragments):
    for f in fragments:
        fid = hashlib.sha256(f["content"].encode()).hexdigest()[:16]
        try:
            conn.execute(
                "INSERT OR IGNORE INTO fragments (id, content, source, tier, ingested_at) VALUES (?,?,?,?,?)",
                (fid, f["content"], f["source"], f["tier"], datetime.utcnow().isoformat())
            )
        except Exception:
            pass
    conn.commit()

def extract_text(html, selector="article, .content, main, .man-page, #content, body"):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    el = soup.select_one(selector) or soup.body
    return el.get_text(separator="\n", strip=True) if el else ""

# ── Git ──
def scrape_git():
    print("\n=== Scraping Git docs ===")
    frags = []
    
    # Git core docs
    try:
        r = requests.get("https://api.github.com/repos/git/git/contents/Documentation", headers=HEADERS)
        if r.status_code == 200:
            files = [item["name"] for item in r.json() if item["name"].endswith(".txt") or item["name"].endswith(".adoc")]
        else:
            files = ["git-add.txt","git-branch.txt","git-commit.txt","git-checkout.txt","git-merge.txt","git-rebase.txt","git-log.txt","git-status.txt","git-pull.txt","git-push.txt","git-clone.txt","git-config.txt","git-diff.txt","git-fetch.txt","git-reset.txt","git-stash.txt","git-tag.txt","git-remote.txt","git-submodule.txt"]
            
        for f in files:
            url = f"https://raw.githubusercontent.com/git/git/master/Documentation/{f}"
            text = fetch(url)
            if not text: continue
            for c in chunk_text(text):
                frags.append({"content": c, "source": f"git/{f}@github.com", "tier": "A"})
            print(f"  git/{f}: {len(chunk_text(text))} chunks")
    except Exception as e:
        print(f"  Git core error: {e}")

    # Pro Git Book
    try:
        chapters = ["01-introduction","02-git-basics","03-git-branching","04-git-server","05-distributed-git","06-github","07-git-tools","08-customizing-git","09-git-and-other-systems","10-git-internals"]
        for ch in chapters:
            url = f"https://api.github.com/repos/progit/progit2/contents/book/{ch}"
            r = requests.get(url, headers=HEADERS)
            if r.status_code == 200:
                for item in r.json():
                    if item["name"].endswith(".asc"):
                        text = fetch(item["download_url"])
                        if not text: continue
                        for c in chunk_text(text):
                            frags.append({"content": c, "source": f"progit/{ch}/{item['name']}@github.com", "tier": "A"})
                        print(f"  progit/{item['name']}: {len(chunk_text(text))} chunks")
    except Exception as e:
        print(f"  ProGit error: {e}")

    return frags

# ── Bash (GNU manual) ──
def scrape_bash():
    print("\n=== Scraping Bash manual ===")
    html = fetch("https://tiswww.case.edu/php/chet/bash/bashref.html")
    if not html: 
        html = fetch("https://www.gnu.org/software/bash/manual/bash.html")
    if not html: return []
    
    # Split by h2 tags as requested by Hermes
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "nav"]):
        tag.decompose()
        
    frags = []
    # If the page is structured with h2
    sections = soup.find_all("h2")
    if sections:
        for i in range(len(sections)):
            sec_title = sections[i].get_text(strip=True)
            content = []
            curr = sections[i].next_sibling
            while curr and curr.name != "h2":
                if hasattr(curr, "get_text"):
                    content.append(curr.get_text(separator="\n", strip=True))
                curr = curr.next_sibling
            
            text = "\n".join(content)
            for c in chunk_text(text, 1200):
                frags.append({"content": f"{sec_title}\n{c}", "source": "bash/manual@tiswww.case.edu", "tier": "A"})
    else:
        text = extract_text(html)
        for c in chunk_text(text, 1200):
            frags.append({"content": c, "source": "bash/manual@tiswww.case.edu", "tier": "A"})
            
    print(f"  Total: {len(frags)} chunks")
    return frags

# ── Docker (docs.docker.com) ──
def scrape_docker():
    print("\n=== Scraping Docker docs ===")
    pages = [
        "build","commit","compose","container","context","cp","create",
        "diff","events","exec","export","history","image","images",
        "import","info","inspect","kill","load","login","logout",
        "logs","manifest","network","node","pause","plugin","port",
        "ps","pull","push","rename","restart","rm","rmi","run",
        "save","search","secret","service","stack","start","stats",
        "stop","swarm","system","tag","top","unpause","update",
        "version","volume","wait",
    ]
    frags = []
    for p in pages:
        html = fetch(f"https://docs.docker.com/reference/cli/docker/{p}/")
        if not html: continue
        text = extract_text(html)
        for c in chunk_text(text):
            frags.append({"content": c, "source": f"docker/{p}@docs.docker.com", "tier": "A"})
        print(f"  docker {p}: {len(chunk_text(text))} chunks")
    return frags

# ── Kubernetes (kubernetes.io) ──
def scrape_kubernetes():
    print("\n=== Scraping Kubernetes docs ===")
    pages = [
        "overview/","concepts/overview/working-with-objects/",
        "concepts/workloads/pods/","concepts/workloads/controllers/deployment/",
        "concepts/workloads/controllers/statefulset/",
        "concepts/workloads/controllers/daemonset/",
        "concepts/workloads/controllers/job/",
        "concepts/services-networking/service/",
        "concepts/services-networking/ingress/",
        "concepts/storage/volumes/","concepts/storage/persistent-volumes/",
        "concepts/configuration/configmap/","concepts/configuration/secret/",
        "concepts/scheduling-eviction/assign-pod-node/",
        "concepts/cluster-administration/logging/",
        "concepts/security/rbac-good-practices/",
        "tasks/configure-pod-container/configure-liveness-readiness-startup-probes/",
        "tasks/manage-kubernetes-objects/declarative-config/",
        "tasks/run-application/horizontal-pod-autoscale/",
    ]
    frags = []
    for p in pages:
        html = fetch(f"https://kubernetes.io/docs/{p}")
        if not html: continue
        text = extract_text(html)
        for c in chunk_text(text):
            frags.append({"content": c, "source": f"kubernetes/{p.strip('/')}@kubernetes.io", "tier": "A"})
        print(f"  k8s/{p.strip('/')}: {len(chunk_text(text))} chunks")
    return frags

# ── C/C++ (cppreference.com) ──
def scrape_cpp():
    print("\n=== Scraping C/C++ reference ===")
    pages_c = [
        "c/language/main_function","c/language/for","c/language/while",
        "c/language/if","c/language/switch","c/language/struct",
        "c/language/union","c/language/enum","c/language/pointer",
        "c/language/array","c/language/typedef","c/language/operator_precedence",
        "c/io/printf","c/io/scanf","c/io/fopen","c/io/fclose",
        "c/io/fread","c/io/fwrite","c/io/fprintf","c/io/fscanf",
        "c/string/byte/strlen","c/string/byte/strcpy","c/string/byte/strcmp",
        "c/string/byte/strcat","c/string/byte/memcpy","c/string/byte/memset",
        "c/memory/malloc","c/memory/calloc","c/memory/realloc","c/memory/free",
        "c/numeric/math/sqrt","c/numeric/math/pow","c/numeric/math/abs",
    ]
    pages_cpp = [
        "cpp/container/vector","cpp/container/map","cpp/container/unordered_map",
        "cpp/container/set","cpp/container/list","cpp/container/deque",
        "cpp/container/array","cpp/container/stack","cpp/container/queue",
        "cpp/string/basic_string","cpp/io/cout","cpp/io/cin",
        "cpp/io/basic_fstream","cpp/algorithm/sort","cpp/algorithm/find",
        "cpp/algorithm/binary_search","cpp/algorithm/for_each",
        "cpp/algorithm/transform","cpp/algorithm/accumulate",
        "cpp/memory/unique_ptr","cpp/memory/shared_ptr","cpp/memory/weak_ptr",
        "cpp/thread/thread","cpp/thread/mutex","cpp/thread/lock_guard",
        "cpp/language/lambda","cpp/language/range-for","cpp/language/auto",
        "cpp/language/class","cpp/language/templates","cpp/language/exceptions",
        "cpp/utility/optional","cpp/utility/variant","cpp/utility/tuple",
    ]
    frags = []
    for p in pages_c + pages_cpp:
        html = fetch(f"https://en.cppreference.com/w/{p}")
        if not html: continue
        text = extract_text(html, "#mw-content-text")
        lang = "c" if p.startswith("c/") else "cpp"
        for c in chunk_text(text):
            frags.append({"content": c, "source": f"{lang}/{p}@cppreference.com", "tier": "A"})
        print(f"  {p}: {len(chunk_text(text))} chunks")
    return frags

# ── CSS/HTML via MDN ──
def scrape_mdn_web():
    print("\n=== Scraping MDN Web docs (CSS/HTML) ===")
    css_pages = [
        "display","position","flexbox","grid","float","margin","padding",
        "border","background","color","font","text-align","transform",
        "transition","animation","box-shadow","opacity","z-index",
        "overflow","visibility","cursor","pseudo-classes","pseudo-elements",
        "media_queries","variables","calc","clamp","min","max",
        "Specificity","Cascade","Inheritance","Box_model",
    ]
    html_pages = [
        "Element/div","Element/span","Element/a","Element/p","Element/h1",
        "Element/ul","Element/ol","Element/li","Element/table","Element/form",
        "Element/input","Element/button","Element/select","Element/textarea",
        "Element/img","Element/video","Element/audio","Element/canvas",
        "Element/section","Element/article","Element/nav","Element/header",
        "Element/footer","Element/main","Element/aside","Element/details",
        "Element/dialog","Element/template","Element/slot",
    ]
    frags = []
    for p in css_pages:
        html = fetch(f"https://developer.mozilla.org/en-US/docs/Web/CSS/{p}")
        if not html: continue
        text = extract_text(html, "article")
        for c in chunk_text(text):
            frags.append({"content": c, "source": f"css/{p}@developer.mozilla.org", "tier": "A"})
        print(f"  CSS/{p}: {len(chunk_text(text))} chunks")
    for p in html_pages:
        html = fetch(f"https://developer.mozilla.org/en-US/docs/Web/HTML/{p}")
        if not html: continue
        text = extract_text(html, "article")
        for c in chunk_text(text):
            frags.append({"content": c, "source": f"html/{p}@developer.mozilla.org", "tier": "A"})
        print(f"  HTML/{p}: {len(chunk_text(text))} chunks")
    return frags

# ── Java (dev.java) ──
def scrape_java():
    print("\n=== Scraping Java docs ===")
    pages = [
        "learn/getting-started/","learn/language-basics/variables.html",
        "learn/language-basics/operators.html","learn/language-basics/control-flow.html",
        "learn/classes-objects/","learn/interfaces/",
        "learn/generics/","learn/exceptions/","learn/lambdas/",
        "learn/streams/","learn/collections/","learn/date-time/",
        "learn/io/","learn/concurrency/","learn/modules/",
        "learn/records/","learn/sealed-classes/","learn/pattern-matching/",
    ]
    frags = []
    for p in pages:
        html = fetch(f"https://dev.java/{p}")
        if not html: continue
        text = extract_text(html)
        for c in chunk_text(text):
            frags.append({"content": c, "source": f"java/{p.strip('/')}@dev.java", "tier": "A"})
        print(f"  java/{p.strip('/')}: {len(chunk_text(text))} chunks")
    return frags

# ── Terraform ──
def scrape_terraform():
    print("\n=== Scraping Terraform docs via Next.js API ===")
    build_id = "FXi0hUxaqLnv8ZF2NuF7g"
    pages = [
        "terraform/language", "terraform/language/resources/syntax", "terraform/language/data-sources",
        "terraform/language/values/variables", "terraform/language/values/outputs", "terraform/language/values/locals",
        "terraform/language/state", "terraform/language/settings", "terraform/language/functions",
        "terraform/language/expressions", "terraform/language/modules", "terraform/language/providers",
        "terraform/language/style", "terraform/language/syntax", "terraform/language/meta-arguments",
        "terraform/cli/commands/init", "terraform/cli/commands/plan", "terraform/cli/commands/apply",
        "terraform/cli/commands/destroy", "terraform/cli/commands/import", "terraform/cli/commands/validate",
        "terraform/cli/commands/workspace/list", "terraform/cli/commands/state", "terraform/intro"
    ]
    frags = []
    for p in pages:
        url = f"https://developer.hashicorp.com/_next/data/{build_id}/{p}.json"
        raw = fetch(url)
        if not raw: continue
        try:
            data = json.loads(raw)
            mdx = data["pageProps"]["mdxSource"]["compiledSource"]
            
            # Very basic extraction: regex to strip out jsx components/JS functions
            # In a real pipeline we'd evaluate the MDX or use a robust JSX stripper, 
            # but string manipulation gets the core textual content.
            text = re.sub(r'var \w+ = function[^{]+{[^}]+};', '', mdx)
            text = re.sub(r'return _jsx.*?\);', '', text, flags=re.DOTALL)
            text = re.sub(r'<[^>]+>', '', text)  # strip tags if any
            text = re.sub(r'\\n', '\n', text)
            
            for c in chunk_text(text):
                frags.append({"content": c, "source": f"{p}@developer.hashicorp.com", "tier": "A"})
            print(f"  {p}: {len(chunk_text(text))} chunks")
        except Exception as e:
            print(f"  Terraform JSON error on {p}: {e}")
    return frags

# ── C# / .NET ──
def scrape_csharp():
    print("\n=== Scraping C# docs ===")
    pages = [
        "csharp/fundamentals/types/","csharp/fundamentals/object-oriented/",
        "csharp/fundamentals/functional/pattern-matching",
        "csharp/language-reference/keywords/","csharp/language-reference/operators/",
        "csharp/language-reference/statements/","csharp/programming-guide/generics/",
        "csharp/asynchronous-programming/","csharp/linq/",
        "csharp/programming-guide/exceptions/",
        "csharp/programming-guide/delegates/",
        "csharp/programming-guide/events/",
    ]
    frags = []
    for p in pages:
        html = fetch(f"https://learn.microsoft.com/en-us/dotnet/{p}")
        if not html: continue
        text = extract_text(html)
        for c in chunk_text(text):
            frags.append({"content": c, "source": f"csharp/{p}@learn.microsoft.com", "tier": "A"})
        print(f"  csharp/{p}: {len(chunk_text(text))} chunks")
    return frags

# ── SQL (generic) ──
def scrape_sql():
    print("\n=== Scraping SQL reference ===")
    frags = []
    
    # PostgreSQL raw SGML
    try:
        r = requests.get("https://api.github.com/repos/postgres/postgres/contents/doc/src/sgml/ref", headers=HEADERS)
        if r.status_code == 200:
            pg_files = [item["name"] for item in r.json() if item["name"].endswith(".sgml")]
        else:
            pg_files = ["select.sgml", "insert.sgml", "create_table.sgml", "abort.sgml", "alter_aggregate.sgml", "alter_database.sgml", "alter_domain.sgml"]
            
        for p in pg_files:
            url = f"https://raw.githubusercontent.com/postgres/postgres/master/doc/src/sgml/ref/{p}"
            text = fetch(url)
            if not text: continue
            # Basic SGML strip
            text = re.sub(r'<[^>]+>', '', text)
            for c in chunk_text(text):
                frags.append({"content": c, "source": f"sql/postgres/{p}@github.com", "tier": "A"})
            print(f"  sql/postgres/{p}: {len(chunk_text(text))} chunks")
    except Exception as e:
        print(f"  Postgres error: {e}")

    # SQLite single pages
    sqlite_pages = ["lang.html", "vdbe.html", "fileformat2.html", "arch.html", "queryplanner.html", "fts3.html", "json1.html", "rtree.html"]
    for p in sqlite_pages:
        html = fetch(f"https://www.sqlite.org/{p}")
        if not html: continue
        text = extract_text(html, ".fancy")
        for c in chunk_text(text):
            frags.append({"content": c, "source": f"sql/sqlite/{p}@sqlite.org", "tier": "A"})
        print(f"  sql/sqlite/{p}: {len(chunk_text(text))} chunks")
        
    # W3Schools
    w3_pages = ["select","insert","update","delete","alter","create_table","drop_table","constraints","view","groupby","having","null_values","operators"]
    for p in w3_pages:
        html = fetch(f"https://www.w3schools.com/sql/sql_{p}.asp")
        if not html: continue
        text = extract_text(html, "#main")
        for c in chunk_text(text):
            frags.append({"content": c, "source": f"sql/w3schools/{p}@w3schools.com", "tier": "B"})
        print(f"  sql/w3schools/{p}: {len(chunk_text(text))} chunks")
        
    return frags

# ── Kotlin ──
def scrape_kotlin():
    print("\n=== Scraping Kotlin docs ===")
    pages = [
        "basic-syntax.html","basic-types.html","strings.html",
        "control-flow.html","returns.html","classes.html",
        "inheritance.html","properties.html","interfaces.html",
        "data-classes.html","sealed-classes.html","generics.html",
        "enum-classes.html","object-declarations.html","delegation.html",
        "functions.html","lambdas.html","collections-overview.html",
        "sequences.html","coroutines-overview.html","null-safety.html",
        "exceptions.html","annotations.html","destructuring-declarations.html",
        "scope-functions.html","operator-overloading.html",
    ]
    frags = []
    for p in pages:
        html = fetch(f"https://kotlinlang.org/docs/{p}")
        if not html: continue
        text = extract_text(html, "article")
        for c in chunk_text(text):
            frags.append({"content": c, "source": f"kotlin/{p}@kotlinlang.org", "tier": "A"})
        print(f"  kotlin/{p}: {len(chunk_text(text))} chunks")
    return frags

# ── Swift ──
def scrape_swift():
    print("\n=== Scraping Swift docs (Raw Markdown) ===")
    frags = []
    
    directories = ["GuidedTour", "LanguageGuide", "LanguageReference"]
    for d in directories:
        try:
            r = requests.get(f"https://api.github.com/repos/swiftlang/swift-book/contents/TSPL.docc/{d}", headers=HEADERS)
            if r.status_code == 200:
                files = [item["name"] for item in r.json() if item["name"].endswith(".md")]
            else:
                files = []
                
            for f in files:
                url = f"https://raw.githubusercontent.com/swiftlang/swift-book/main/TSPL.docc/{d}/{f}"
                text = fetch(url)
                if not text: continue
                for c in chunk_text(text):
                    frags.append({"content": c, "source": f"swift/{d}/{f}@github.com", "tier": "A"})
                print(f"  swift/{f}: {len(chunk_text(text))} chunks")
        except Exception as e:
            print(f"  Swift error on {d}: {e}")
            
    return frags

# ═══════════════════════════════════════
# Main
# ═══════════════════════════════════════
ALL_SCRAPERS = [
    ("Git", scrape_git),
    ("Bash", scrape_bash),
    ("Docker", scrape_docker),
    ("Kubernetes", scrape_kubernetes),
    ("C/C++", scrape_cpp),
    ("CSS/HTML", scrape_mdn_web),
    ("Java", scrape_java),
    ("Terraform", scrape_terraform),
    ("C#", scrape_csharp),
    ("SQL", scrape_sql),
    ("Kotlin", scrape_kotlin),
    ("Swift", scrape_swift),
]

def main():
    # Allow running specific scrapers via CLI args
    targets = sys.argv[1:] if len(sys.argv) > 1 else None

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    before = conn.execute("SELECT COUNT(*) as c FROM fragments").fetchone()["c"]
    print(f"Starting count: {before} fragments")

    total_new = 0
    for name, fn in ALL_SCRAPERS:
        if targets and name.lower() not in [t.lower() for t in targets]:
            continue
        try:
            frags = fn()
            if frags:
                insert_fragments(conn, frags)
                total_new += len(frags)
                print(f"  -> Inserted {len(frags)} fragments for {name}")
        except Exception as e:
            print(f"  ERROR in {name}: {e}")

    # Rebuild FTS
    print("\nRebuilding FTS5 index...")
    conn.execute("INSERT INTO bible_fts(bible_fts) VALUES('rebuild')")
    conn.commit()

    after = conn.execute("SELECT COUNT(*) as c FROM fragments").fetchone()["c"]
    print(f"\nDone! {before} -> {after} fragments (+{after - before} new)")
    conn.close()

if __name__ == "__main__":
    main()
