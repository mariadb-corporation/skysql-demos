# Chat With SkySQL Database Agents

Examples for using SkySQL database agents from popular GenAI frameworks such as LangChain, LlamaIndex, and CrewAI.

These examples provide a conversational layer on top of SkySQL DB agents so you can ask questions in natural language, inspect generated SQL, and integrate database-aware reasoning into a larger application flow.

## Why Use SkySQL DB Agents

Generating accurate SQL for real-world schemas is hard. Production databases often need more than table names and column metadata:

- domain-specific business terminology
- table and column curation
- semantic hints and disambiguation
- safe access patterns and a managed execution layer

SkySQL DB agents provide a managed way to discover schema context, curate relevant tables, and expose the result through APIs that are easier to plug into higher-level AI frameworks.

## Create A SkySQL DB Agent

1. Sign in to the SkySQL portal.
2. Launch a SkySQL database or register an existing MariaDB or MySQL data source.
3. Create a new database agent and describe its goal.
4. Review the generated context, table selection, and semantic hints.
5. Test and refine the agent in the SkySQL playground.

After that, the agent can be called from your own apps and orchestration layers.

## Setup

From this directory:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set the required environment variables before running the examples:

```bash
export OPENAI_API_KEY="your-openai-key"
export SKYSQL_API_KEY="your-skysql-api-key"
```

## Run The Examples

### CLI

LlamaIndex:

```sh
python db_chat_agent.py
```

LangGraph:

```sh
python db_chat_agent_langgraph.py
```

CrewAI:

```sh
python db_chat_agent_crewai.py
```

All CLI variants support listing available DB agents, chatting with them, and showing SQL used to answer a request.

### Streamlit Web App

```sh
streamlit run db_chat_streamlit_app.py
```

Open the local URL printed by Streamlit and chat with the agent in the browser.

### Embeddable Chat Widget

1. Start the backend API:

```sh
python db_chat_api.py
```

By default this serves on `http://localhost:8000`.

2. Serve the static widget files:

```sh
cd static
python3 -m http.server 9000
```

3. Open [http://localhost:9000/test_chat_widget.html](http://localhost:9000/test_chat_widget.html) to test locally.

4. Embed the widget in your own page:

```html
<script src="URL_TO/chat_widget.js"></script>
```

Make sure the API is reachable from the browser and CORS is configured for your deployment.

## Features

- Chat with SkySQL-backed DB agents from CLI or web UI.
- Show generated SQL for transparency.
- Integrate with multiple orchestration frameworks.
- Use a simple embeddable widget for browser-based demos.

## Troubleshooting

- Confirm that `OPENAI_API_KEY` and `SKYSQL_API_KEY` are valid and exported in your shell.
- Re-run `pip install -r requirements.txt` if you hit missing dependency errors.
- Upgrade Streamlit if the web app does not start cleanly.

## Credits

Built with [SkySQL](https://skysql.com), [Streamlit](https://streamlit.io/), [LangGraph](https://github.com/langchain-ai/langgraph), [CrewAI](https://github.com/crewAIInc/crewAI), and [LangChain](https://github.com/langchain-ai/langchain).