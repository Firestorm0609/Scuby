# 🤖 Gemini AI Agent

A free-tier AI agent with tool-calling capabilities running on your VPS.

## Setup

1. Get a free API key at https://aistudio.google.com/apikey
2. Add it to `.env`:
   ```
   GEMINI_API_KEY=your_key_here
   ```

## Usage

### Interactive mode
```bash
python3 agent.py
```

### Single task mode
```bash
python3 agent.py "search for the latest Python 3.13 features"
```

## Available Tools

| Tool | Description |
|------|-------------|
| `web_search` | Search the web |
| `web_fetch` | Fetch and read a URL |
| `read_file` | Read a file from workspace |
| `write_file` | Write to a file |
| `list_files` | List workspace files |
| `run_python` | Execute Python code |
| `run_shell` | Run shell commands |

## Configuration (.env)

```bash
GEMINI_API_KEY=your_key     # Required
GEMINI_MODEL=gemini-2.0-flash  # Default model
MAX_AGENT_STEPS=15          # Max tool calls per task
```
