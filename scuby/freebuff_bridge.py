"""
freebuff_bridge.py — Bridge between Scuby and the Freebuff CLI.

This module lets Scuby use the Freebuff CLI for free AI access.
The CLI runs locally on the VPS and handles all AI calls through
Freebuff's free tier (subsidized by ads).

How it works:
1. Spawns `freebuff` as a subprocess
2. Sends messages via stdin
3. Reads responses from stdout
4. Returns the AI response to Scuby

Usage:
    from freebuff_bridge import freebuff_chat
    reply = await freebuff_chat("hello, what can you do?")
"""

import asyncio
import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Freebuff CLI path
FREEBUFF_BIN = "freebuff"

# Session file to maintain conversation context
SESSION_DIR = Path(tempfile.gettempdir()) / "scuby_freebuff"
SESSION_DIR.mkdir(exist_ok=True)


async def freebuff_chat(message: str, user_id: int = 0) -> str:
    """
    Send a message to Freebuff CLI and get a response.
    
    Args:
        message: The user's message
        user_id: User ID for session tracking
        
    Returns:
        The AI response from Freebuff
    """
    try:
        # Create a unique session file for this user
        session_file = SESSION_DIR / f"session_{user_id}.json"
        
        # Build the freebuff command
        # Use --free flag to ensure free mode
        cmd = [
            FREEBUFF_BIN,
            "--free",
            "--non-interactive",  # If supported
        ]
        
        # Write the message to a temp file
        msg_file = SESSION_DIR / f"msg_{user_id}.txt"
        msg_file.write_text(message)
        
        # Run freebuff with the message
        # Freebuff CLI accepts prompts via stdin or args
        proc = await asyncio.create_subprocess_exec(
            FREEBUFF_BIN,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "NO_COLOR": "1"},
        )
        
        # Send the message and close stdin
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(input=message.encode()),
            timeout=60,  # 60s timeout
        )
        
        response = stdout.decode("utf-8", errors="replace").strip()
        error = stderr.decode("utf-8", errors="replace").strip()
        
        if response:
            return response
        elif error:
            logger.warning(f"Freebuff stderr: {error[:200]}")
            return f"Freebuff error: {error[:200]}"
        else:
            return "Freebuff returned empty response."
            
    except asyncio.TimeoutError:
        return "Freebuff timed out. Try again."
    except FileNotFoundError:
        return "Freebuff CLI not found. Run: npm i -g freebuff"
    except Exception as e:
        logger.error(f"freebuff_chat error: {e}", exc_info=True)
        return f"Freebuff error: {str(e)[:200]}"


async def freebuff_chat_stream(message: str, user_id: int = 0):
    """
    Stream a response from Freebuff CLI.
    
    Yields chunks of the response as they arrive.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            FREEBUFF_BIN,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "NO_COLOR": "1"},
        )
        
        # Send the message
        proc.stdin.write(message.encode())
        await proc.stdin.drain()
        proc.stdin.close()
        
        # Read output line by line
        full_response = ""
        while True:
            line = await asyncio.wait_for(
                proc.stdout.readline(),
                timeout=60,
            )
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            if text:
                full_response += text + "\n"
                yield text
        
        await proc.wait()
        
    except asyncio.TimeoutError:
        yield "Freebuff timed out."
    except Exception as e:
        yield f"Freebuff error: {str(e)[:200]}"


def check_freebuff_installed() -> dict:
    """Check if Freebuff CLI is installed and working."""
    import shutil
    
    result = {
        "installed": False,
        "version": None,
        "path": None,
        "error": None,
    }
    
    path = shutil.which(FREEBUFF_BIN)
    if not path:
        result["error"] = "freebuff not found in PATH"
        return result
    
    result["installed"] = True
    result["path"] = path
    
    try:
        import subprocess
        proc = subprocess.run(
            [FREEBUFF_BIN, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        result["version"] = proc.stdout.strip()
    except Exception as e:
        result["error"] = str(e)
    
    return result
