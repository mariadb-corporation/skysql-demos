# skysql-demos

Public demos showcasing SkySQL technologies, workflows, and developer integrations.

Each demo lives in its own top-level directory with its own setup instructions and required dependencies. The repository is intentionally lightweight: clone it, choose the demo you want to explore, and follow that demo's README.

## Demo Catalog

| Demo | What it shows | Stack |
| --- | --- | --- |
| [`chat-with-db-agents`](chat-with-db-agents/README.md) | Chat with SkySQL database agents from CLI, Streamlit, or an embeddable widget using popular GenAI frameworks. | Python, Streamlit, FastAPI, LangChain, LlamaIndex, CrewAI |
| [`performance-insights-lite`](performance-insights-lite/README.md) | SQL-native lightweight performance insights for MariaDB and SkySQL, including top SQL, wait classes, and short-term rollups. | MariaDB SQL, `mariadb` CLI, shell scripts |

## How To Use This Repo

1. Clone the repository:

```bash
git clone https://github.com/mariadb-corporation/skysql-demos.git
cd skysql-demos
```

2. Pick a demo directory.
3. Follow the README inside that demo for prerequisites, environment variables, and run commands.

## Repo Conventions

- Each demo keeps its own dependencies and setup steps local to its directory.
- Example env files may be committed as `.env.example`.
- Real credentials, `.env` files, and service-specific secrets should never be committed.
- Demos are intended to be easy to inspect, run, and adapt rather than packaged as one shared product.

## Quick Links

- SkySQL product site: [skysql.com](https://skysql.com)
- SkySQL API keys: [app.skysql.com/user-profile/api-keys](https://app.skysql.com/user-profile/api-keys)
- Issues and feedback: [mariadb-corporation/skysql-demos/issues](https://github.com/mariadb-corporation/skysql-demos/issues)
