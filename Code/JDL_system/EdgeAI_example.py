import requests
import json
import sys
import re

try:
  # Enriched RDF data (Turtle) with vehicles, cameras, detection events, timestamps
  ttl_data = """
  @prefix ex: <http://example.org/> .
  @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
  @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
  @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

  # Classes
  ex:Street a rdfs:Class .
  ex:Vehicle a rdfs:Class .
  ex:Car a rdfs:Class ; rdfs:subClassOf ex:Vehicle .
  ex:Truck a rdfs:Class ; rdfs:subClassOf ex:Vehicle .
  ex:Camera a rdfs:Class .
  ex:DetectionEvent a rdfs:Class .

  # Properties
  ex:appearsOn a rdf:Property ; rdfs:domain ex:Vehicle ; rdfs:range ex:Street .
  ex:capturedBy a rdf:Property ; rdfs:domain ex:DetectionEvent ; rdfs:range ex:Camera .
  ex:vehicle a rdf:Property ; rdfs:domain ex:DetectionEvent ; rdfs:range ex:Vehicle .
  ex:street a rdf:Property ; rdfs:domain ex:DetectionEvent ; rdfs:range ex:Street .
  ex:timestamp a rdf:Property ; rdfs:domain ex:DetectionEvent ; rdfs:range xsd:dateTime .
  ex:color a rdf:Property ; rdfs:domain ex:Vehicle ; rdfs:range rdfs:Literal .

  # Streets
  ex:StreetA a ex:Street ; rdfs:label "Street A" .
  ex:StreetB a ex:Street ; rdfs:label "Street B" .

  # Cameras
  ex:CamA a ex:Camera ; rdfs:label "Camera A" .
  ex:CamB a ex:Camera ; rdfs:label "Camera B" .

  # Vehicles
  ex:Car1 a ex:Car ; ex:color "red" ; ex:appearsOn ex:StreetA ; rdfs:label "Red Car" .
  ex:Car2 a ex:Car ; ex:color "blue" ; ex:appearsOn ex:StreetA ; rdfs:label "Blue Car" .
  ex:Truck1 a ex:Truck ; ex:color "white" ; ex:appearsOn ex:StreetB ; rdfs:label "White Truck" .

  # Detection events
  ex:Event1 a ex:DetectionEvent ; ex:vehicle ex:Car1 ; ex:street ex:StreetA ; ex:capturedBy ex:CamA ; ex:timestamp "2024-05-01T08:15:00Z"^^xsd:dateTime .
  ex:Event2 a ex:DetectionEvent ; ex:vehicle ex:Car1 ; ex:street ex:StreetA ; ex:capturedBy ex:CamA ; ex:timestamp "2024-05-01T09:30:00Z"^^xsd:dateTime .
  ex:Event3 a ex:DetectionEvent ; ex:vehicle ex:Car2 ; ex:street ex:StreetA ; ex:capturedBy ex:CamB ; ex:timestamp "2024-05-02T10:05:00Z"^^xsd:dateTime .
  ex:Event4 a ex:DetectionEvent ; ex:vehicle ex:Truck1 ; ex:street ex:StreetB ; ex:capturedBy ex:CamB ; ex:timestamp "2024-05-01T10:00:00Z"^^xsd:dateTime .
  """.strip()

  system_prompt = (
    "You are a SPARQL expert. Given RDF data and a natural-language question, "
    "return ONLY a valid SPARQL query fenced in ```sparql code block (no prose). "
    "Use prefixes from the data when possible."
  )

  user_prompt = (
    "RDF (Turtle):\n\n" + ttl_data +
    "\n\nQuestion: For Street A only, count detections of vehicles grouped by vehicle, "
    "filter to detections between 2024-05-01T08:00:00Z and 2024-05-02T00:00:00Z (inclusive), "
    "and return vehicle labels with their counts, ordered by count desc then label asc, limit 10."
  )

  response = requests.post(
    url="https://openrouter.ai/api/v1/chat/completions",
    headers={
      "Authorization": "Bearer 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",
      "Content-Type": "application/json",
      "HTTP-Referer": "<YOUR_SITE_URL>", # Optional. Site URL for rankings on openrouter.ai.
      "X-Title": "<YOUR_SITE_NAME>", # Optional. Site title for rankings on openrouter.ai.
    },
    data=json.dumps({
      "model": "openai/gpt-oss-20b:free",
      "messages": [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
      ],
      "temperature": 0.0
    }),
    timeout=30
  )
  response.raise_for_status()

  try:
    payload = response.json()
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    # Show request latency
    try:
      latency_s = response.elapsed.total_seconds()
      print(f"\n===== Latency =====\n\nLLM latency: {latency_s:.3f}s\n")
    except Exception:
      pass

    # Try to extract SPARQL from the assistant message content
    def get_assistant_contents(p):
      contents = []
      try:
        choices = p.get("choices") or []
        for c in choices:
          msg = c.get("message") or {}
          content = msg.get("content")
          if isinstance(content, str) and content.strip():
            contents.append(content)
      except Exception:
        pass
      return contents

    def extract_sparql(text):
      # Prefer fenced blocks like ```sparql ... ``` or ```SPARQL ... ```
      fence_pattern = r"```\s*sparql\s*\n([\s\S]*?)```|```\s*\n([\s\S]*?)```"
      m = re.search(fence_pattern, text, re.IGNORECASE)
      if m:
        code = m.group(1) or m.group(2)
        return code.strip()
      # Heuristic: lines that look like SPARQL (SELECT/ASK/CONSTRUCT/DESCRIBE)
      lines = text.strip().splitlines()
      if any(lines) and re.search(r"^(SELECT|ASK|CONSTRUCT|DESCRIBE)\b", lines[0], re.IGNORECASE):
        return "\n".join(lines).strip()
      return None

    all_contents = get_assistant_contents(payload)
    sparql_blocks = []
    for content in all_contents:
      code = extract_sparql(content)
      if code:
        sparql_blocks.append(code)

    if sparql_blocks:
      print("\n===== SPARQL =====\n")
      # Print the first SPARQL block; print more if multiple
      for idx, code in enumerate(sparql_blocks, start=1):
        label = f"# Query {idx}\n" if len(sparql_blocks) > 1 else ""
        print(f"{label}```sparql\n{code}\n```\n")
  except ValueError:
    # Response is not JSON
    print(response.text)
except requests.RequestException as e:
  print(f"Request failed: {e}", file=sys.stderr)
  if 'response' in locals() and response is not None:
    try:
      print(json.dumps(response.json(), indent=2, ensure_ascii=False), file=sys.stderr)
    except Exception:
      print(response.text, file=sys.stderr)