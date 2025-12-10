# 🧠 Named Entity Recognition (NER) with spaCy

This guide explains how to install **spaCy**, download a language model, and extract named entities from text along with their types.

---

## 📦 1. Installation

First, install **spaCy** using `pip`:

```bash
pip install spacy
```

Then download a small English model:

```bash
python -m spacy download en_core_web_sm
```

---

## ⚙️ 2. Loading the Model

After installation, load the model in Python:

```python
import spacy

# Load the English NLP model
nlp = spacy.load('en_core_web_sm')
```

---

## 🧩 3. Extracting Entities from Text

You can now use the model to process text and extract entities:

```python
# Example text
text = "Elon Musk founded SpaceX in the United States."

# Process the text
doc = nlp(text)

# Extract entities and their types
for ent in doc.ents:
    print(f"{ent.text:<15} {ent.label_}")
```

### 💡 Output Example

```
Elon Musk      PERSON
SpaceX         ORG
United States  GPE
```

---

## 🗝️ 4. Entity Type Reference

| Entity Code | Description |
|--------------|-------------|
| **PERSON** | People, including fictional |
| **NORP** | Nationalities, religious or political groups |
| **FAC** | Buildings, airports, highways, bridges, etc. |
| **ORG** | Companies, agencies, institutions, etc. |
| **GPE** | Countries, cities, states |
| **LOC** | Non-GPE locations (mountains, bodies of water, etc.) |
| **PRODUCT** | Objects, vehicles, foods, etc. (not services) |
| **EVENT** | Named hurricanes, battles, wars, sports events, etc. |
| **WORK_OF_ART** | Titles of books, songs, etc. |
| **LAW** | Named documents made into laws |
| **LANGUAGE** | Any named language |
| **DATE** | Absolute or relative dates or periods |
| **TIME** | Times smaller than a day |
| **PERCENT** | Percentage, including "%" |
| **MONEY** | Monetary values, including unit |
| **QUANTITY** | Measurements (weight, distance, etc.) |
| **ORDINAL** | “first”, “second”, etc. |
| **CARDINAL** | Numerals that do not fall under another type |

---

## ✅ Example Summary

Here’s a complete working example combining everything:

```python
import spacy

# Load model
nlp = spacy.load('en_core_web_sm')

# Input text
text = "Barack Obama was born in Hawaii and served as President of the United States."

# Process and print entities
doc = nlp(text)
for ent in doc.ents:
    print(f"{ent.text:<20} {ent.label_}")
```

---

**Example Output:**

```
Barack Obama          PERSON
Hawaii                GPE
President             ORG
United States         GPE
```

---

> 🧩 **Tip:** Try experimenting with other texts or larger models (like `en_core_web_md`) for better accuracy.
