# Verification Prompt

You are a verification system. Given a list of extracted knowledge items and their source paragraphs, determine whether each extraction faithfully represents the source material.

## Input

```json
{{extracted_items}}
```

## Instructions

For each item, compare the `title` and `description` against the `source_paragraph`. Assess whether the extraction accurately captures what the source paragraph states.

## Verdict Criteria

- **confirmed**: The extraction accurately and faithfully represents the source paragraph. The claims in the title and description are directly supported by the text.
- **uncertain**: The extraction is plausible but overstates, understates, generalizes beyond, or slightly misrepresents what the source paragraph says. Includes cases where the source is ambiguous.
- **rejected**: The extraction does not reflect the source paragraph. The claimed finding, hypothesis, method, dataset, or gap cannot be derived from the provided text.

## Rules

1. Be strict. If the source paragraph does not clearly and directly support the extraction, mark it `uncertain`.
2. If the source paragraph is missing or empty, mark `rejected`.
3. Evaluate each item independently.
4. The `reason` field must be a single sentence explaining the verdict.
5. Do not re-extract or suggest corrections. Only evaluate.

## Output Format

Return ONLY the following JSON structure. No commentary, no markdown fences, no explanation.

```json
{
  "items": [
    {
      "title": "...",
      "verdict": "confirmed|uncertain|rejected",
      "reason": "..."
    }
  ]
}
```
