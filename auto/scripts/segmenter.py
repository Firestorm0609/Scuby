"""
Splits a script into sentence chunks and extracts a Pexels search keyword
for each one using the Anthropic API (claude-sonnet-4-6).

Falls back to simple noun-extraction if no ANTHROPIC_API_KEY is set.
"""
import re
import os
import json
import urllib.request
import urllib.error
import config


def _sentences(text: str) -> list[str]:
    """Split text into individual sentences."""
    parts = re.split(r'(?<=[.!?])\s+', text.strip())
    return [p.strip() for p in parts if p.strip()]


def chunk_script(text: str, sentences_per_chunk: int = None) -> list[str]:
    """
    Group sentences into chunks. Uses config.SENTENCES_PER_CHUNK if set,
    otherwise defaults to 2.
    """
    n = sentences_per_chunk or getattr(config, "SENTENCES_PER_CHUNK", 2)
    sents = _sentences(text)
    chunks = []
    for i in range(0, len(sents), n):
        chunks.append(" ".join(sents[i:i + n]))
    return chunks


# ── keyword extraction ────────────────────────────────────────────────────────

# Used only when a chunk has zero usable subject words left after
# filtering (e.g. it's pure filler like "Okay so yeah that's it").
# Deliberately generic/neutral so it reads fine as a cartoon scene
# regardless of the video's actual topic — unlike config.STOCK_QUERY,
# which is a Pexels *photo* search term and not cartoon-appropriate.
_GENERIC_TOPIC = "a curious idea forming"


def _keywords_via_claude(chunks: list[str]) -> list[str]:
    """
    Ask Claude to pick one concrete, visual Pexels search term per chunk.
    Returns a list of keywords in the same order as chunks.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("[segmenter] No ANTHROPIC_API_KEY set — skipping Claude keyword "
              "extraction, using free fallback instead.")
        return []

    prompt = (
        "You are helping pick stock photo search terms for a narration video.\n"
        "For each numbered text chunk below, reply with ONE short (1-3 word) "
        "concrete visual search term suitable for Pexels — something you could "
        "actually photograph. Prefer specific nouns over abstract ideas.\n"
        "Reply ONLY with a JSON array of strings, same order as the chunks.\n\n"
        + "\n".join(f"{i+1}. {c}" for i, c in enumerate(chunks))
    )

    body = json.dumps({
        "model": "claude-sonnet-4-6",
        "max_tokens": 300,
        "messages": [{"role": "user", "content": prompt}]
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
        raw = data["content"][0]["text"].strip()
        # strip markdown fences if present
        raw = re.sub(r"^```json|```$", "", raw, flags=re.MULTILINE).strip()
        keywords = json.loads(raw)
        if isinstance(keywords, list) and len(keywords) == len(chunks):
            print(f"[segmenter] Claude keywords: {keywords}")
            return keywords
        print(f"[segmenter] Claude returned malformed keyword list "
              f"(expected {len(chunks)} items) — falling back")
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode()[:300]
        except Exception:
            pass
        if e.code in (401, 403):
            print(f"[segmenter] Claude API auth error ({e.code}) — "
                  f"check ANTHROPIC_API_KEY is valid. {body} — falling back")
        else:
            print(f"[segmenter] Claude API error {e.code}: {body} — falling back")
    except Exception as e:
        print(f"[segmenter] Claude keyword extraction failed: {e} — falling back")
    return []


def _keywords_fallback(chunks: list[str]) -> list[str]:
    """
    No-API-key heuristic: build a short descriptive PHRASE per chunk
    (not a single isolated word), filtering out names, pronouns,
    transition words, and other non-visual filler that previously
    leaked through as "topics" (e.g. "that's", "today", "benjamin").

    Strategy:
      1. Strip a large stop/filler list (pronouns, auxiliaries, time
         words, discourse markers, common first-name leakage).
      2. Score remaining words by length + frequency (longer, repeated
         words are usually the real subject of the sentence).
      3. Take the top 2-4 surviving words *in their original order* so
         the result reads as a coherent phrase, not a word salad.
      4. If nothing survives, fall back to a generic phrase built from
         config.STOCK_QUERY rather than a single meaningless word.
    """
    stopwords = {
        # articles / conjunctions / prepositions
        "the","a","an","and","or","but","in","on","at","to","for","of","with",
        "as","by","from","into","over","about","after","before","between",
        "through","both","up","down","out","off","than","so","yet","nor",
        # auxiliaries / verbs-to-be
        "is","was","are","were","be","been","being","have","has","had",
        "do","does","did","will","would","could","should","can","may",
        "might","must","shall",
        # pronouns
        "it","its","it's","they","them","their","theirs","he","him","his",
        "she","her","hers","we","us","our","ours","you","your","yours",
        "i","me","my","mine","this","that","that's","these","those",
        "who","whom","whose","what","which",
        # discourse / filler / time words common in narration scripts
        "there","here","when","where","how","why","not","no","yes","just",
        "even","also","still","only","very","really","quite","more","most",
        "some","any","each","every","all","one","two","three","first",
        "second","third","next","then","now","today","tomorrow","yesterday",
        "okay","ok","kay","yeah","yep","yup","nah","well","actually",
        "basically","literally","like","alright","right","sure",
        "thing","things","something","someone","somebody","anything",
        "understood","understand","know","think","said","says","say",
        "want","wants","wanted","need","needs","needed","let's","let",
        "going","gonna","got","get","gets","getting","kind","sort",
    }

    def _phrase_for(chunk: str) -> str:
        # tokenize while tracking sentence-start position, so we can tell
        # "Today" (sentence-initial capital — just normal capitalisation)
        # apart from "Benjamin" (mid-sentence capital — likely a proper name)
        tokens = list(re.finditer(r"[A-Za-z']+", chunk))
        if not tokens:
            return _GENERIC_TOPIC

        cleaned = []
        is_likely_name = []
        for tok in tokens:
            w = tok.group().strip("'")
            cleaned.append(w)
            # mid-sentence capitalised word = likely a proper name, not a topic
            preceding = chunk[:tok.start()]
            at_sentence_start = bool(re.search(r"(^\s*|[.!?]\s+)$", preceding))
            is_likely_name.append(
                w[:1].isupper() and not at_sentence_start and len(w) > 1
            )

        kept_idx = [
            i for i, w in enumerate(cleaned)
            if w.lower() not in stopwords and len(w) > 3
        ]
        if not kept_idx:
            return _GENERIC_TOPIC

        from collections import Counter
        freq = Counter(cleaned[i].lower() for i in kept_idx)

        # score: frequency + length, but heavily penalise one-off
        # mid-sentence capitalised words (likely names) so they don't
        # outrank the chunk's actual subject matter
        def _score(i):
            name_penalty = -100 if (is_likely_name[i] and freq[cleaned[i].lower()] == 1) else 0
            return (name_penalty, freq[cleaned[i].lower()], len(cleaned[i]))

        scored = sorted(kept_idx, key=_score, reverse=True)

        # dedupe (case-insensitive) in score order first — so among
        # repeats of the same word we keep the highest-scoring pick —
        # then restore left-to-right order for a natural-reading phrase
        seen = set()
        selected_idx = []
        for i in scored:
            key = cleaned[i].lower()
            if key not in seen:
                seen.add(key)
                selected_idx.append(i)
            if len(selected_idx) == 4:
                break

        if not selected_idx:
            return _GENERIC_TOPIC
        selected_idx.sort()
        return " ".join(cleaned[i] for i in selected_idx)

    keywords = [_phrase_for(c) for c in chunks]
    print(f"[segmenter] No ANTHROPIC_API_KEY / Claude unavailable — "
          f"using free fallback phrases: {keywords}")
    return keywords


def extract_keywords(chunks: list[str]) -> list[str]:
    """Try Claude first, fall back to heuristic."""
    kw = _keywords_via_claude(chunks)
    if kw:
        return kw
    return _keywords_fallback(chunks)
