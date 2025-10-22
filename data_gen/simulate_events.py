"""
Streaming-style log simulator for a Member Insights analytics demo.

Generates four CSVs under data/raw:
  - members.csv
  - titles.csv
  - play_events.csv
  - search_events.csv
"""
from __future__ import annotations

import argparse, uuid, random
from pathlib import Path
from datetime import datetime, timedelta, date
import numpy as np
import pandas as pd

#updated imports
#import random
#import uuid
#from dataclasses import dataclass
#from datetime import datetime, timedelta, date
#from pathlib import Path
#from typing import Dict, List

#import numpy as np
#import pandas as pd

REGIONS = ["NA", "LATAM", "EMEA", "APAC"]
REGION_P = [0.40, 0.20, 0.25, 0.15]
DEVICES = ["tv", "mobile", "tablet", "desktop"]
ACTIONS = ["play","pause", "resume", "stop", "complete"]
SEARCH_TYPES = ["title", "genre", "actor"]

# DEVICE_P = [0.50, 0.25, 0.15, 0.10]

# GENRES = [
    "Drama", "Comedy", "Action", "Thriller", "Romance", "Sci-Fi",
    "Documentary", "Family", "Horror", "Animation", "Fantasy",
]




#@dataclass(frozen=True)
#class Anomaly:
#    day: date
#    key: str        # "device" or "region"
#    value: str
#    drop: float

#    @staticmethod
#    def parse(spec: str) -> "Anomaly":
#        # Example: 2025-08-15:device=tv:drop=0.5
#       dpart, kv, ddrop = spec.split(":")
#        k, v = kv.split("=")
#        _, frac = ddrop.split("=")
#        return Anomaly(
#            day=datetime.strptime(dpart, "%Y-%m-%d").date(),
#            key=k, value=v, drop=float(frac)
#        )

def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


#def daterange(days: int, end: date | None = None):
#    end = end or datetime.utcnow().date()
#    start = end - timedelta(days=days)
#    cur = start
#    while cur < end:
#        yield cur
#        cur += timedelta(days=1)

def weekend_lift(d: date) -> float:
    return 1.25 if d.weekday() >= 5 else 1.0

def simulate_members(n: int, rng: np.random.Generator) -> pd.DataFrame:
    signup_origin = pd.Timestamp("2020-01-01")
    return pd.DataFrame({
        "member_id": [str(uuid.uuid4()) for _ in range(n)],
        "region": rng.choice(REGIONS, size=n, p=REGION_P),
        "signup_date": pd.to_datetime(rng.integers(0, 365 * 5, size=n), unit="D", origin=signup_origin).date,
        "plan": rng.choice(["basic", "standard", "premium"], size=n, p=[0.25, 0.45, 0.30]),
    })

def simulate_titles(n: int, rng: np.random.Generator) -> pd.DataFrame:
    return pd.DataFrame({
        "title_id": [str(uuid.uuid4()) for _ in range(n)],
        "genre": rng.choice(["Drama", "Comedy", "Doc", "Action", "Kids"], size=n),
        "minutes": rng.integers(5, 140, size=n),
        "is_series": rng.choice([0, 1], size=n, p=[0.6, 0.4]),
    })



def simulate_play_events(members: pd.DataFrame, titles: pd.DataFrame, days: int, rng: np.random.Generator) -> pd.DataFrame:
    start = (datetime.utcnow().date() - timedelta(days=days-1))
    events = []
    member_ids = members["member_id"].values
    title_ids - titles["title_id"}.values
                       for d in daterange(start,datetime.utcnow().date()):
                       scale = ind(0.15*len(member_ids)*weekend_lift(d))
                       for _ in range(scale):
                        m = rng.choice(member_ids)
                        t = rng.choce(title_ids)
                        action_path = ["play"] + rng.choice([["complete"], ["stop"], ["pause", "resume" "complete", ]], p=[0.5, 0.2, 0.3])
                        base_time = datetime.combine(d, datetime.min.time() + timedelta(minutes=int(rng.integers(0, 24*60))))
                        minutes = int(rng.integers(1,60))
                       device = rng.choice(DEVICES, p=[0.45, 0.35, 0.1, 0.1])
                       for i, a in enumerate(action_path):
                       events.append({
                                     "event_time": (base_time + timedelta(minutes=i)).isoformat(sep=" "),
                                     "member_id": m,
                                     "title_id": t,
                                     "action": a,
                                     "minutes_watched": minutes if a in ("play", "resume", "complete") else 0,
                                     "device": device,
                
                                })
                       return pd.DataFrame(events)


    #rows: List[Dict] = []
    #for day in daterange(days):
    #    base_active = 0.12 * weekend_lift(day)
    #    active_members = members.sample(frac=min(base_active, 0.9), random_state=(hash(day) % (2**32)))
    #    for _, m in active_members.iterrows():
    #        sessions = int(rng.integers(1, 4))
    #        for _ in range(sessions):
    #            t = titles.sample(1, random_state=int(rng.integers(1e9))).iloc[0]
    #            session_minutes = int(rng.integers(5, min(180, int(t["minutes"]) + 30)))
    #            actions = ["play"]
    #            if rng.random() < 0.35:
    #                actions.append("pause")
    #            if "pause" in actions and rng.random() < 0.85:
    #                actions.append("resume")
    #            if session_minutes > int(t["minutes"]) * 0.85 and rng.random() < 0.9:
    #                actions.append("complete")
    #            actions.append("stop")
    #            for a in actions:
    #                rows.append({
    #                    "event_time": datetime.combine(day, datetime.min.time()) + timedelta(minutes=int(rng.integers(0, 24 * 60))),
    #                    "member_id": m["member_id"],
    #                    "title_id": t["title_id"],
    #                    "action": a,
    #                    "minutes_watched": session_minutes if a in ("stop", "complete") else 0,
    #                    "device": rng.choice(DEVICES, p=DEVICE_P),
    #                })
    # return pd.DataFrame(rows)


def simulate_search_events(days: int, members: pd.DataFrame, rng: np.random.Generator) -> pf.DataFrame:
                       start = (datetime.utcnow().date() - timedelta(days=days-1))
                       rows = []
                       qs = ["motor racing", "cooking", "space", "cat videos", "fitness", "history", "news"]
                       for d in daterange(start, datetime.utcnow().date())
                       for _ in range(int(0.88 * len(members) * weekend_lift)):
                       "event_time": datetime.combine(d, datetime.min.time()+timedelta(minutes=int(np.random.randint(0, 24*60)))),
                       "member_id": members["member_id"].sample(1).values[0],
                       "search_type" np.random.choice(SEARCH_TYPES, p=[0.85, 0.1, 0.05]),
                       "query": random.choice(qs),
                       
                    }]


#def simulate_search_events(members: pd.DataFrame, days: int, rng: np.random.Generator) -> pd.DataFrame:
#    vocab = ["love", "space", "war", "family", "crime", "nature", "comedy", "night", "fast", "sport"]
#    rows: List[Dict] = []
#    for day in daterange(days):
#        active = members.sample(frac=0.20 * weekend_lift(day), random_state=((hash(day) + 7) % (2**32)))
#        for _, m in active.iterrows():
#            for _ in range(int(rng.integers(0, 3))):
#                rows.append({
#                    "event_time": datetime.combine(day, datetime.min.time()) + timedelta(minutes=int(rng.integers(0, 24 * 60))),
#                    "member_id": m["member_id"],
#                    "search_type": rng.choice(SEARCH_TYPES, p=[0.6, 0.3, 0.1]),
#                    "query": " ".join(rng.choice(vocab, size=int(rng.integers(1, 3)))).strip(),
#               })
#    return pd.DataFrame(rows)




def apply_anomalies(df: pd.DataFrame, anomalies: List[Anomaly], rng: np.random.Generator) -> pd.DataFrame:
    if not anomalies:
        return df
    out = df
    for a in anomalies:
        mask = pd.to_datetime(out["event_time"]).dt.date.eq(a.day)
        if a.key == "device" and "device" in out.columns:
            mask &= out["device"].eq(a.value)
        drop_mask = (rng.random(out.shape[0]) < a.drop) & mask.to_numpy()
        out = out.loc[~drop_mask].reset_index(drop=True)
    return out

def main(args: argparse.Namespace) -> None:
    out_raw = Path("data/raw")
    out_raw.mkdir(parents=True, exist_ok=True)

    seed = args.seed if args.seed is not None else 42
    rng = np.random.default_rng(seed)
    random.seed(seed)

    members = simulate_members(args.members, rng)
    titles = simulate_titles(args.titles, rng)
    play_events = simulate_play_events(members, titles, args.days, rng)
    search_events = simulate_search_events(members, args.days, rng)

    anomalies = [Anomaly.parse(s) for s in (args.anomaly or [])]
    if anomalies:
        play_events = apply_anomalies(play_events, anomalies, rng=rng)

    members.to_csv(out_raw / "members.csv", index=False)
    titles.to_csv(out_raw / "titles.csv", index=False)
    play_events.to_csv(out_raw / "play_events.csv", index=False)
    search_events.to_csv(out_raw / "search_events.csv", index=False)

    print("Wrote:")
    for p in ["members.csv", "titles.csv", "play_events.csv", "search_events.csv"]:
        print("  -", (out_raw / p).as_posix())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate streaming-style logs for Member Insights")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--members", type=int, default=5000)
    parser.add_argument("--titles", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--anomaly", action="append", help="e.g. 2025-08-15:device=tv:drop=0.5 (repeatable)")
    main(parser.parse_args())
