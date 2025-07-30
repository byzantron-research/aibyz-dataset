This repository is a curated dataset for analyzing validator behavior in Proof-of-Stake (PoS) networks like Ethereum 2.0, Cosmos, and Polkadot. It includes both real-world and synthetic data to support machine learning, anomaly detection, and multi-agent reinforcement learning research.

---

## 📦 What's Inside

### 1. **Real-World Data**

Collected from public APIs and platforms:

* **Ethereum 2.0** (Beaconcha.in, Rated.network)
* **Cosmos** (Cosmos SDK)
* **Polkadot** (Subscan.io, Dune Analytics)

Metrics: uptime, missed attestations, proposals, slashing, stake amounts.

### 2. **Synthetic Data**

Simulated using **SimPy** and **NetworkX**:

* Validator behaviors: honest, lazy, selfish, Sybil, long-range attacks
* Labeled logs for supervised learning and testing

### 3. **Feature Enrichment**

Added features like:

* Trust scores
* Message entropy
* Consensus deviation
* Behavior labels (rule-based)

### 4. **Exported Dataset**

* Formats: CSV, JSON
* Includes schema, metadata, and versioning

---

## 📁 Structure

```
validator-dataset/
├── data/                # Real, synthetic, enriched, final
├── scripts/             # Collection, simulation, feature engineering
├── assets/              # Diagrams and visuals
├── schema/              # Field definitions
├── LICENSE
└── README.md
```

---
