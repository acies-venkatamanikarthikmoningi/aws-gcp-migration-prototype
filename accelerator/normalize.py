"""Unified migration knowledge model: entity resolution across inventory, IaC, code, docs and diagrams.

Rules (deterministic, explainable):
  * match by resource_id, then exact name, then CloudFormation logical id/name alias
  * merge evidence; never silently pick a value – differing attributes become `conflicts`
  * confidence = 1 - prod(1 - c_i) across independent sources
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .catalogue import category_of

COMPARE_ATTRS = {  # attribute -> how to read it from (data, configuration)
    "region": lambda d, c: d.get("region") if d.get("region") not in (None, "global") else None,
    "service": lambda d, c: d.get("service") if not str(d.get("service", "")).startswith(("UNMAPPED", "UNKNOWN")) else None,
    "instance_type": lambda d, c: c.get("instance_type") or c.get("instance_class") or c.get("InstanceType"),
    "engine_version": lambda d, c: c.get("engine_version") or c.get("EngineVersion"),
    "runtime": lambda d, c: (c.get("runtime") or c.get("Runtime") or
                             (f"python{c['engine_version']}" if c.get("engine") == "python" and c.get("engine_version") else None)),
}
SOURCE_RANK = {"inventory": 3, "dependency": 3, "code": 2, "document": 1, "diagram": 1, "ownership": 3}


def _norm(v: Any) -> str:
    return str(v).strip().lower()


def resolve(evidence: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """evidence rows carry: entity_type, data, confidence, source_location, category(upload category), upload_id."""
    resources: dict[str, dict] = {}
    by_name: dict[str, str] = {}
    aliases: dict[str, str] = {}  # CFN logical id / iac address -> key

    def add_source(r: dict, e: dict) -> None:
        r["evidence"].append({"category": e["category"], "location": e["source_location"],
                              "confidence": e["confidence"], "upload_id": e.get("upload_id")})
        cfg = e["data"].get("configuration") or {}
        for attr, fn in COMPARE_ATTRS.items():
            v = fn(e["data"], cfg)
            if v not in (None, ""):
                r["_attr_obs"][attr].append((str(v), e["source_location"], e["category"]))
        for k in ("application", "environment", "criticality", "region"):
            if not r.get(k) and e["data"].get(k) and e["data"].get(k) != "global":
                r[k] = e["data"][k]
        if cfg:
            r["configuration"].setdefault(e["category"], {}).update(cfg)

    # pass 1: inventory resources are anchors
    ordered = sorted([e for e in evidence if e["entity_type"] == "resource"],
                     key=lambda e: -SOURCE_RANK.get(e["category"], 0))
    for e in ordered:
        d = e["data"]
        key = None
        rid = d.get("resource_id")
        name = d.get("name")
        if rid and rid in resources:
            key = rid
        elif name and _norm(name) in by_name:
            key = by_name[_norm(name)]
        if key is None:
            key = rid or f"{e['category'][:3]}:{name}"
            resources[key] = {"resource_id": key, "name": name or key, "service": None, "region": d.get("region"),
                              "application": d.get("application"), "environment": d.get("environment"),
                              "criticality": d.get("criticality"), "configuration": {}, "evidence": [],
                              "_attr_obs": defaultdict(list)}
            if name:
                by_name[_norm(name)] = key
        r = resources[key]
        if d.get("iac_address"):
            aliases[_norm(d["iac_address"])] = key
            aliases[_norm(d["iac_address"].split(".")[-1])] = key
        add_source(r, e)

    # finalize resources
    out = []
    for key, r in resources.items():
        obs = r.pop("_attr_obs")
        conflicts = []
        for attr, vals in obs.items():
            distinct = {v.lower() for v, _, _ in vals}
            if len(distinct) > 1:
                conflicts.append({"attribute": attr, "observations": [{"value": v, "source": loc, "category": c}
                                                                      for v, loc, c in vals]})
        svcs = [v for v, _, c in sorted(obs.get("service", []), key=lambda x: -SOURCE_RANK.get(x[2], 0))]
        r["service"] = svcs[0] if svcs else "UNKNOWN"
        cats = {ev["category"] for ev in r["evidence"]}
        p_miss = 1.0
        for ev in r["evidence"]:
            p_miss *= (1 - ev["confidence"])
        r["confidence"] = round(1 - p_miss, 3)
        if conflicts:
            status = "CONFLICT"
        elif "inventory" in cats and len(cats) > 1:
            status = "CORROBORATED"
        elif "inventory" in cats:
            status = "INVENTORY_ONLY"
        elif "code" in cats:
            status = "IAC_ONLY"
        else:
            status = "DIAGRAM_OR_DOC_ONLY"
        r["evidence_status"] = status
        r["conflicts"] = conflicts
        r["category"] = category_of(r["service"]) if r["service"] != "UNKNOWN" else "unknown"
        out.append(r)

    # application attribution for IaC-only resources: majority application of resources declared in the SAME file
    # (marked inferred – never presented as confirmed)
    file_apps: dict[str, Counter] = defaultdict(Counter)
    for r in out:
        for ev in r["evidence"]:
            f = ev["location"].split(":")[0]
            if r.get("application"):
                file_apps[f][r["application"]] += 1
    for r in out:
        if not r.get("application"):
            votes = Counter()
            for ev in r["evidence"]:
                votes.update(file_apps.get(ev["location"].split(":")[0], Counter()))
            if votes:
                r["application"] = votes.most_common(1)[0][0]
                r["application_inferred"] = True

    # consumers (non-AWS downstream systems)
    consumers: dict[str, dict] = {}
    for e in evidence:
        if e["entity_type"] == "consumer" and e["data"].get("node_id"):
            d = e["data"]
            consumers[d["node_id"]] = {"node_id": d["node_id"], "display_name": d.get("display_name"),
                                       "application": d.get("application"), "consumer_type": d.get("consumer_type"),
                                       "evidence": [e["source_location"]]}
    display_to_node = {_norm(c["display_name"]): nid for nid, c in consumers.items() if c.get("display_name")}

    def node_for(ref: str) -> str:
        n = _norm(ref)
        if ref in resources:
            return ref
        if n in by_name:
            return by_name[n]
        if n in aliases:
            return aliases[n]
        if n in display_to_node:
            return display_to_node[n]
        if ref in consumers:
            return ref
        return ref  # unresolved stays as-is (flagged)

    rels: dict[tuple, dict] = {}
    for e in evidence:
        if e["entity_type"] != "relationship":
            continue
        s, t = node_for(e["data"]["source"]), node_for(e["data"]["target"])
        if s == t:
            continue
        k = (s, t)
        rel = rels.setdefault(k, {"source": s, "target": t, "types": set(), "evidence": [], "confidence": 0.0})
        rel["types"].add(e["data"]["type"])
        rel["evidence"].append({"category": e["category"], "location": e["source_location"], "confidence": e["confidence"]})
    known = set(resources) | set(consumers)
    rel_out = []
    for rel in rels.values():
        p = 1.0
        for ev in rel["evidence"]:
            p *= (1 - ev["confidence"])
        rel["confidence"] = round(1 - p, 3)
        cats = {ev["category"] for ev in rel["evidence"]}
        explicit = any(ev["confidence"] >= 0.95 for ev in rel["evidence"])
        rel["status"] = "CONFIRMED" if explicit or len(cats) >= 2 else "INFERRED"
        rel["unresolved"] = [x for x in (rel["source"], rel["target"]) if x not in known]
        if rel["unresolved"]:
            rel["status"] = "NEEDS_REVIEW"
        types = sorted(rel["types"])
        explicit_types = [t for t in types if not t.startswith(("DIAGRAM", "DOC", "IAC"))]
        rel["relationship_type"] = explicit_types[0] if explicit_types else types[0]
        rel["types"] = types
        rel_out.append(rel)
    stats = {"resources": len(out), "relationships": len(rel_out), "consumers": len(consumers),
             "conflicts": sum(1 for r in out if r["conflicts"]),
             "by_status": {s: sum(1 for r in out if r["evidence_status"] == s) for s in
                           ("CORROBORATED", "INVENTORY_ONLY", "IAC_ONLY", "DIAGRAM_OR_DOC_ONLY", "CONFLICT")}}
    return out, rel_out, {"consumers": list(consumers.values()), "stats": stats}
