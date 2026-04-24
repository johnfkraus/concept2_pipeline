# The Concept2 Logbook Pipeline

*Or: What Happens to Your Workout After You Press "Stop" on the Erg*

---

Let me tell you something interesting.

Every time you finish a piece on a Concept2 rowing machine, the machine knows
things about you. It knows how far you went, how long it took, how many times
per minute you pulled the handle, how hard your heart was working. All of that
gets sent up to a server at Concept2 — their Logbook — and it sits there,
waiting.

Now here's the thing most people don't think about: *what is a workout,
really, from the computer's point of view?* It's just a bunch of numbers with
a timestamp. Distance. Time. Strokes. Pace. And those numbers are sitting on
someone else's computer in a format called JSON — which is just a very
particular way of writing down a list of facts, like a structured note to
yourself that a machine can read.

This program goes and gets those facts, stores them in a sensible place, and
then turns them into something you can look at. Let me walk you through
exactly how it does that, step by step, because the steps are interesting.

---

## The Core Idea: You Can't Always Start From Scratch

Here's a question. You've got five years of workouts — call it two thousand
sessions. Every morning at six o'clock, you want to update your local copy
with whatever you did yesterday. Do you download all two thousand workouts
again?

Of course not. That would be like re-reading the entire encyclopedia every
time you wanted to add a new entry. What you do instead is remember where you
left off.

This program does exactly that. Before it asks Concept2 for anything, it looks
at what it already has and finds the most recent date. Then it says to the
API: *"Give me everything after this date."* If you did one workout yesterday,
you get one workout's worth of data. The rest stays put. That's called
**incremental loading**, and it's one of those ideas that sounds obvious once
you hear it but that a surprising number of systems get wrong.

---

## The Three Places Data Lives: Bronze, Silver, Gold

Now, here's a design choice that's worth understanding, because it comes from
a genuine insight about how data gets messy.

When a workout comes back from the Concept2 API, it looks something like this:

```json
{
  "id": 123456,
  "date": "2026-04-23 07:14:00",
  "distance": 8000,
  "time": 19823,
  "type": "rower",
  "heart_rate": { "average": 158, "max": 172, "min": 141 },
  "workout": { "splits": [...] }
}
```

That's the raw truth — exactly what Concept2 said. And here's the thing about
raw truth: it's valuable precisely *because* nobody has touched it. The moment
you start transforming it — parsing the date, renaming fields, computing pace
— you're making choices, and choices can be wrong. If you only kept the
transformed version and your transformation had a bug, the original information
is gone.

So we keep both. We keep the raw version first, in a place called the **bronze
layer**. Then we build a clean version on top of it, called **silver**. Then
we build the pretty pictures on top of *that*, called **gold**. If something
goes wrong in silver, bronze is still there, unchanged. You can re-run silver
as many times as you like.

---

## Layer One: Bronze — The Raw Archive (OpenSearch)

**What it does:** Asks the Concept2 API for your workouts and stores them,
word for word, in OpenSearch.

**OpenSearch** is a search engine and document store — think of it as a very
fast filing cabinet that can hold JSON documents and find them quickly. Each
workout gets stored as its own document, identified by its Concept2 workout ID.

That last part is important. If you run the program twice, it doesn't create
two copies of the same workout. It just overwrites the existing document with
the same ID. In the database world, we call this **idempotent** — the Latin
root means "same power," meaning you can apply it as many times as you want
and the result is always the same. That's a very good property to have.

The bronze layer also stores everything Concept2 sends — the nested heart rate
object, the split arrays, the stroke data. Nothing gets thrown away. Future
you might want fields that present you decided were unimportant.

```
┌─────────────────────────────────────────────────────┐
│  Concept2 API  →  bronze_workouts asset  →           │
│  OpenSearch index: concept2_workouts_raw             │
│                                                      │
│  One document per workout. _id = Concept2 workout id │
│  Raw JSON, nothing transformed, nothing discarded    │
└─────────────────────────────────────────────────────┘
```

The **incremental cursor** lives here. Before each run, the code asks
OpenSearch: *"What's the most recent date you have?"* That date goes into the
API request as `updated_after`, and only newer workouts come back.

---

## Layer Two: Silver — The Clean Table (PostgreSQL)

**What it does:** Reads everything from the bronze OpenSearch index, cleans
it up, and writes it into a proper relational database table.

This is where the interesting work happens. The raw API data is a bit rough
around the edges:

- `time` is stored in **tenths of a second**. A 20-minute row is not 1200,
  it's 12000. We convert it.
- `date` comes back as a string: `"2026-04-23 07:14:00"`. We parse it into a
  real timestamp, and we also pull out the year, month, and day of the week as
  separate columns — because you're going to want to ask questions like "how
  far did I row in March?" and having those as columns makes that fast.
- `heart_rate` is a nested object. We flatten it into three simple integer
  columns: `heart_rate_avg`, `heart_rate_max`, `heart_rate_min`.
- **Pace** doesn't come from the API directly. We compute it: take the
  duration in seconds, divide by distance in meters, multiply by 500. That's
  your pace per 500 meters — the number every rower cares about.
- Activity type gets normalised: "bike" and "bikeerg" both mean the same
  machine. We pick one.

The result is a PostgreSQL table called `concept2_silver.workouts` with clean,
typed columns you can query directly.

The write is safe to repeat. Every row uses the workout ID as its primary key,
and the insert command says: *"If a row with this ID already exists, update it
instead of adding a duplicate."* In SQL this is `ON CONFLICT DO UPDATE`. It
means you can re-run silver any time without creating a mess.

```
┌──────────────────────────────────────────────────────┐
│  OpenSearch  →  silver_workouts asset  →              │
│  PostgreSQL: concept2_silver.workouts                 │
│                                                       │
│  Typed columns. Derived fields. Upsert on workout_id  │
│  Safe to re-run. No duplicates, ever.                 │
└──────────────────────────────────────────────────────┘
```

---

## Layer Three: Gold — The Charts (Plotly)

**What it does:** Queries the silver table and generates charts.

This part is straightforward. The data is already clean and typed. We select
what we want — distance over time, pace trend, workout frequency by month,
heart rate distribution — and hand it to Plotly, which draws the pictures.
Each chart is saved as both a PNG (for embedding in documents) and an HTML
file (interactive, hoverable, zoomable).

```
┌──────────────────────────────────────────────────────┐
│  PostgreSQL  →  gold_charts asset  →                  │
│  charts_output/*.png + *.html                         │
│                                                       │
│  The point of the whole exercise: something to look at│
└──────────────────────────────────────────────────────┘
```

---

## What Dagster Does

You might be asking: why not just write three Python functions and call them in
order?

You could. But then you'd have to answer some awkward questions. What if bronze
succeeded but silver crashed halfway through? Do you re-run bronze too? What if
you want to run silver and gold every hour but only bronze once a day? What if
you want to look at the history of how many rows silver produced last Tuesday?

**Dagster** is a system for answering those questions. It knows about the
dependencies between your assets — bronze must run before silver, silver before
gold — and it draws them as a graph in a web UI you can look at and click
through. It keeps a log of every run. It lets you set a schedule. It has a
sensor that watches for bronze to finish and automatically kicks off silver and
gold.

Think of Dagster as the stage manager. The assets are the performers. The stage
manager doesn't do the rowing; it makes sure everyone is in the right place at
the right time, and it keeps the records.

The pipeline runs automatically every morning at **06:00 UTC**. You can also
trigger it manually from the Dagster UI at `http://localhost:3000`.

---

## Getting It Running

### What You Need

- Python 3.11 or newer
- **Podman** on macOS: `brew install podman` — for running OpenSearch locally
- A Concept2 Logbook account with an API application registered at
  [log.concept2.com/developers/home](https://log.concept2.com/developers/home)

### Step 1 — Install

```bash
git clone <your-repo>
cd concept2_pipeline
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Step 2 — Configure

```bash
cp .env.example .env
# Open .env and fill in your Concept2 credentials
```

The variables you need:

| Variable | What it is |
|---|---|
| `C2_CLIENT_ID` | From the Concept2 developer portal |
| `C2_CLIENT_SECRET` | Also from the developer portal |
| `C2_ACCESS_TOKEN` | Obtained in Step 3 below |
| `OS_HOST` | `http://localhost:9200` (default) |
| `OS_USER` | `admin` (default) |
| `OS_PASSWORD` | Set when you start OpenSearch |
| `PG_CONN` | PostgreSQL connection string |

### Step 3 — Start OpenSearch

```bash
chmod +x run_opensearch.sh
./run_opensearch.sh
```

This pulls the OpenSearch container image, starts it in Podman with a
persistent volume so your data survives restarts, and waits until the cluster
is healthy. It prints the connection details when it's ready.

To stop it later: `./run_opensearch.sh stop`

Or, to start the full stack (OpenSearch + PostgreSQL + the Dashboards UI)
with Docker Compose:

```bash
docker-compose up -d
```

OpenSearch Dashboards — a browser UI for exploring your raw workout documents
— will be available at `http://localhost:5601`.

### Step 4 — Start PostgreSQL

If you're not using docker-compose for the full stack:

```bash
brew services start postgresql@15
createdb concept2
psql concept2 -c "CREATE USER concept2 WITH PASSWORD 'concept2pass';"
psql concept2 -c "GRANT ALL ON DATABASE concept2 TO concept2;"
```

### Step 5 — Get Your Access Token

```bash
python -m concept2_pipeline.auth
```

A browser window opens. You log in to Concept2, approve the app, and the token
is printed. Copy it into `C2_ACCESS_TOKEN` in your `.env`.

The OAuth handshake is the standard two-step dance: Concept2 proves it's
talking to your app (via client ID and secret), and you prove it's talking to
your account (via the browser login). The token it hands back is what the
pipeline uses for every subsequent API call.

### Step 6 — Run Dagster

```bash
dagster dev
```

Navigate to `http://localhost:3000`. You'll see three assets in the lineage
graph: `bronze_workouts`, `silver_workouts`, `gold_charts`, connected by
arrows showing the data flow. Click **Materialize All** to run the full pipeline
for the first time.

On the first run, bronze fetches your complete workout history. Every run after
that fetches only what's new.

---

## Poking at the Data Directly

While the pipeline is running or after it completes, you can look at the data
directly.

**OpenSearch — raw workout documents:**

```bash
# How many workouts are stored?
curl -u admin:C0ncept2Search! http://localhost:9200/concept2_workouts_raw/_count

# The 5 most recent, sorted by date
curl -u admin:C0ncept2Search! \
  http://localhost:9200/concept2_workouts_raw/_search \
  -H 'Content-Type: application/json' \
  -d '{"size": 5, "sort": [{"date": {"order": "desc"}}]}'
```

**PostgreSQL — the clean silver table:**

```bash
psql $PG_CONN -c "
  SELECT workout_date, activity_type_clean, distance_km,
         duration_seconds / 60.0 AS minutes,
         pace_sec_per_500m
  FROM concept2_silver.workouts
  ORDER BY workout_date DESC
  LIMIT 10;
"
```

---

## A Note on the Design

One thing worth saying clearly: the pipeline is built so that each layer is
**independently repairable**.

If the silver transform has a bug — say, pace was computed wrong for a while —
you fix the bug, wipe the silver table, and re-run silver. Bronze is
untouched; all your raw data is still there. You don't have to re-download
anything from Concept2.

If you decide the gold charts need a different color scheme, or a new chart
entirely, you just re-run gold. Silver is untouched.

This separation between storage and transformation is the whole point of the
medallion architecture. It's not bureaucracy for its own sake. It's an
acknowledgment that our understanding of the data changes over time, and we
shouldn't have to pay the full cost of re-ingestion every time we change our
minds about how to clean it.

The data is real. The transformations are opinions. Opinions should be cheap to
revise. That's the idea.
