# Steam ML Dataset Guide

This project generates **multi-format CSV datasets** from Steam's top-300 most-rated games, optimized for machine learning projects.

## Output Files

### 1. **steam_ml_dataset.csv** ← START HERE FOR ML
**Best for:** Predictive modeling, classification, regression
- `appId`: Unique Steam app identifier
- `metacritic_score`: Critical score (0-100, nullable)
- `total_reviews`: Count of user reviews (all ratings combined)
- `positive_ratio`: % positive reviews (0.0–1.0)
- `price`: USD price (0 for free, nullable for region-locked)
- `discount_percent`: Current discount (0 = no discount)
- `is_free`: Binary flag (1=free-to-play, 0=paid)
- `num_genres`: Count of game genres (feature engineering)
- `platforms_count`: Count of supported platforms (Windows/Mac/Linux)

**Use Cases:**
- Predict game review sentiment from metadata
- Estimate review counts from price + genres
- Classify games by market positioning (F2P vs paid)
- Analyze platform-support impact on player engagement

---

### 2. **steam_metadata.csv**
**Best for:** Exploratory data analysis, genre/platform analysis
- `appId`, `nombre` (game name)
- `release_date`: Launch date
- `metacritic_score`: Critic rating
- `genres`: Semicolon-separated list (e.g., "Action;Adventure;RPG")
- `platforms`: Supported OS (e.g., "windows;mac;linux")
- `is_free`: Business model flag

**Use Cases:**
- Time-series analysis of game releases by genre
- Platform popularity trends
- Correlate Metacritic with Steam reviews

---

### 3. **steam_reviews.csv**
**Best for:** Sentiment analysis, quality metrics
- `appId`
- `total_reviews`: Total review count
- `positive_ratio`: Proportion of positive reviews (critical feature)
- `review_score`: Steam aggregated score (1–10)

**Use Cases:**
- Sentiment distribution analysis
- Quality/satisfaction scoring models
- Outlier detection (games with divergent critic vs player scores)

---

### 4. **steam_pricing.csv**
**Best for:** Price elasticity, economic analysis
- `appId`
- `price`: USD retail price
- `discount_percent`: Current sale discount
- `is_free`: Free-to-play indicator

**Use Cases:**
- Price elasticity vs review volume
- Discount strategy effectiveness
- F2P vs paid revenue model comparison

---

### 5. **steam_genres.csv**
**Best for:** Genre analysis, co-occurrence networks
- `appId`
- `genre`: Individual genre (one row per genre per app)

**Unique structure:** One-to-many (each app can have multiple genres)

**Use Cases:**
- Genre co-occurrence analysis (what genres appear together?)
- Market saturation by genre
- Genre popularity trends over time

---

### 6. **steam_games_dataset.csv** ← Legacy Combined Format
Consolidated view of all fields from the other CSVs. **Not recommended for ML** due to data redundancy.

---

## Recommended ML Workflows

### Workflow 1: **Predict Positive Review Ratio**
```
Input:  metacritic_score, price, discount_percent, is_free, num_genres, platforms_count
Target: positive_ratio
Model:  Linear Regression, Gradient Boosting
```

### Workflow 2: **Classify Game Quality (Sentiment)**
```
Input:  price, num_genres, platforms_count, is_free
Target: positive_ratio > 0.75 (boolean)
Model:  Logistic Regression, Random Forest
```

### Workflow 3: **Estimate Review Volume**
```
Input:  metacritic_score, price, genre_count, positive_ratio
Target: total_reviews
Model:  Poisson Regression, Gradient Boosting
```

### Workflow 4: **Genre Network Analysis**
```
Input:  steam_genres.csv (edges: app → genre pairs)
Output: Genre co-occurrence matrix, clustering
Model:  Network analysis, dimensionality reduction
```

---

## Data Quality Notes

- **Missing values:** 
  - `metacritic_score`: ~40% nullable (indie/non-reviewed games)
  - `price`: Nullable for region-locked titles
  - `discount_percent`: 0 if no active discount
  
- **Sampling bias:** Top-300 by review count skews toward multiplayer, popular AAA, and F2P (survivor bias)

- **Geographic variation:** Prices vary by region; dataset uses US pricing

---

## Running the Collector

```bash
# Collect top 300 most-rated games
python steam_collector.py

# Modify collection size in steam_collector.py:
# TOP_N_MOST_RATED = 100  # for smaller dataset
# MAX_APPS = None         # collect all ~120K apps (very slow)
```

Expected runtime: **30–60 minutes** for 300 games (respects 1.2s/request rate limit).

---

## Data Import (Pandas)

```python
import pandas as pd

# Load ML-ready dataset
df = pd.read_csv('steam_ml_dataset.csv')

# Handle missing values
df['metacritic_score'].fillna(df['metacritic_score'].median(), inplace=True)
df['price'].fillna(0, inplace=True)

# Feature scaling
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
df[['metacritic_score', 'total_reviews', 'price']] = scaler.fit_transform(
    df[['metacritic_score', 'total_reviews', 'price']]
)

# Load genre relationships
genres = pd.read_csv('steam_genres.csv')
```

---

## Feedback & Improvements

- Add `release_year` for temporal analysis
- Include developer/publisher info for team-based studies
- Fetch historical discount data
- Add player count estimates (external API)
