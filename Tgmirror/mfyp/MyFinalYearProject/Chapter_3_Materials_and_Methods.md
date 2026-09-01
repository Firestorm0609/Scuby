# CHAPTER THREE

## MATERIALS AND METHODS

### 3.1 Introduction

This chapter describes the study area, the data used in this research, and the methodology employed for spatial modeling of solar radiation. The methodology encompasses data collection and preprocessing, geostatistical analysis using six interpolation methods, and cross-validation using ArcGIS Geostatistical Analyst. Detailed mathematical formulations are provided for each interpolation method to ensure reproducibility of the results.

### 3.2 Study Area

#### 3.2.1 Geographic Coverage

The study area covers West Africa and adjacent regions, extending approximately from latitude 4°N to 27°N and from longitude 18°W to 16°E. This vast region encompasses approximately 6.5 million square kilometers and includes a diverse range of climatic zones, from the hyper-arid Sahara Desert in the north through the semi-arid Sahel belt, the sub-humid Sudan and Guinea savanna zones, to the humid tropical forests along the Gulf of Guinea coast.

The study area includes all or portions of the following countries: Mauritania, Mali, Niger, Chad, Senegal, The Gambia, Guinea-Bissau, Guinea, Sierra Leone, Liberia, Côte d'Ivoire, Burkina Faso, Ghana, Togo, Benin, Nigeria, and parts of Cameroon.

#### 3.2.2 Climatic Zones

The study area spans four major climatic zones:

**Saharan Zone (north of 20°N):** Extremely arid with annual rainfall below 100 mm. Clear skies dominate, resulting in high solar radiation throughout the year, typically exceeding 6.5 kWh/m²/day. Dust aerosols from the Sahara can significantly attenuate incoming radiation during the Harmattan season (December–February).

**Sahel Zone (12°N–20°N):** Semi-arid with annual rainfall of 200–600 mm concentrated in the rainy season (June–September). Solar radiation is high during the dry season (November–March) but is reduced during the brief rainy season by increased cloud cover. This zone experiences the largest seasonal variation in solar radiation, with monthly mean SIS ranging from approximately 4,500 Wh/m² during the rainy season to over 6,500 Wh/m² during the dry season.

**Guinea Savanna Zone (6°N–12°N):** Sub-humid with annual rainfall of 600–1,200 mm, with a longer rainy season (April–October) than the Sahel. Solar radiation is moderate, with monthly mean SIS typically ranging from 3,500 to 5,500 Wh/m².

**Coastal/Humid Zone (south of 6°N):** Humid with annual rainfall exceeding 1,200 mm. Persistent cloud cover and high humidity reduce solar radiation, particularly during the rainy season. Annual average SIS is typically in the range of 4.0–5.5 kWh/m²/day, the lowest in the study area.

#### 3.2.3 Topography and Land Cover

The study area exhibits significant topographic variation, from sea level along the coast to over 2,000 m in the Fouta Djallon highlands of Guinea and the Adamawa Plateau of Cameroon. The Jos Plateau in central Nigeria (elevation 1,200–1,400 m) represents a notable topographic feature that influences local solar radiation patterns.

Land cover ranges from dense tropical forests in the south, through savanna woodlands and grasslands, to sparse desert vegetation in the north. These land cover differences affect surface albedo and evapotranspiration, which in turn influence local atmospheric conditions and solar radiation.

#### 3.2.4 Significance for Solar Energy

West Africa receives abundant solar radiation, with most areas receiving more than 4.5 kWh/m²/day annually—well above the threshold considered economically viable for photovoltaic power generation (5.0 kWh/m²/day). The International Renewable Energy Agency (IRENA, 2022) has identified West Africa as having some of the highest solar energy potential in the world, yet the region remains largely untapped due to limited infrastructure and inadequate resource assessment data.

Nigeria, the most populous country in the region, has an installed electricity generation capacity of approximately 13,000 MW, but experiences chronic power shortages due to insufficient generation and transmission infrastructure. Solar energy has been identified as a key pathway for addressing this deficit, with the Nigerian government setting a target of 30% renewable energy by 2030.

### 3.3 Data

#### 3.3.1 Solar Radiation Data

The primary data used in this study consists of monthly mean Surface Incoming Shortwave (SIS) radiation measurements from 108 meteorological stations distributed across West Africa. The data were obtained from the CAMS (Copernicus Atmosphere Monitoring Service) Radiation Archive, which provides satellite-derived and ground-calibrated radiation estimates based on Meteosat observations.

The dataset covers the period from **1983 to 2022**, providing a 40-year record of monthly SIS values in units of Wh/m². Each record contains: station index, year, month, and SIS value. The dataset contains approximately **51,840 monthly station records** (108 stations × 40 years × 12 months), though not all stations have complete records.

#### 3.3.2 Station Network

The 108 meteorological stations are distributed across the study area with the following characteristics:

| Parameter | Value |
|-----------|-------|
| Total stations | 108 |
| Latitude range | 4.38°N – 26.33°N |
| Longitude range | -17.47°E – 15.84°E |
| Countries covered | 17 |

**Table 3.1:** Selected meteorological stations used in the study.

| Station | Latitude (°N) | Longitude (°E) | Country |
|---------|---------------|-----------------|---------|
| Fderik | 22.68 | 12.71 | Mauritania |
| Tessalit | 20.25 | 0.99 | Mali |
| Bilma | 18.69 | 12.92 | Niger |
| Niamey | 13.51 | 2.13 | Niger |
| Bamako | 12.65 | -8.00 | Mali |
| Ouagadougou | 12.37 | 1.52 | Burkina Faso |
| Kano | 12.00 | 8.59 | Nigeria |
| Accra | 5.56 | 0.20 | Ghana |
| Lagos | 6.52 | 3.38 | Nigeria |
| Bissau | 11.86 | -15.58 | Guinea-Bissau |

#### 3.3.3 Data Preprocessing

The raw data underwent the following preprocessing steps:

1. **Missing value identification:** Records with NaN values were identified and excluded. Stations with more than 20% missing data were excluded entirely.

2. **Outlier detection:** Station values deviating by more than three standard deviations from the spatial mean were flagged and reviewed.

3. **Monthly climatology computation:** For each station and month, a climatological mean was computed by averaging across all available years. This reduces inter-annual variability and provides a stable basis for interpolation.

4. **Coordinate validation:** Station coordinates were verified against known geographic positions.

#### 3.3.4 Study Area Map

*[Figure 3.1: Map of the study area showing the distribution of 108 meteorological stations across West Africa. The map shows the four climatic zones (Saharan, Sahel, Guinea Savanna, Coastal) with station locations marked as black dots.]*

### 3.4 Methods

#### 3.4.1 Overview of Interpolation Framework

The spatial modeling framework consists of three stages: (1) computation of monthly climatologies, (2) spatial interpolation using six methods, and (3) cross-validation.

#### 3.4.2 Method 1: Inverse Distance Weighting (IDW)

IDW estimates the SIS value at each grid point as a distance-weighted average:

**Z*(x₀) = Σᵢ [Z(xᵢ) / d(x₀, xᵢ)²] / Σᵢ [1 / d(x₀, xᵢ)²]**

The power parameter p = 2 was used, following Watson and Philip (1985).

#### 3.4.3 Method 2: Ordinary Kriging (OK)

Ordinary Kriging estimates values as a weighted linear combination where weights are determined by the variogram. The exponential variogram model was used:

**γ(h) = c₀ + c[1 - exp(-h/a)]**

The kriging system is solved for each prediction point:

**Σⱼ λⱼ γ(xᵢ, xⱼ) + μ = γ(x₀, xᵢ)**
**Σⱼ λⱼ = 1**

#### 3.4.4 Method 3: Empirical Bayes Kriging (EBK)

EBK follows the approach of Krivoruchko (2012):

1. Log-transform: y = ln(z)
2. Local variogram estimation with automated fitting
3. Kriging in log-space
4. Back-transformation: Z*(x₀) = exp[y*(x₀) + σ²(x₀)/2]

#### 3.4.5 Method 4: Radial Basis Function (RBF)

RBF uses the thin-plate spline: φ(r) = r² ln(r). The surface passes exactly through observed values.

#### 3.4.6 Method 5: Local Polynomial Interpolation (LPI)

LPI fits first-order polynomials within a moving window centered at each prediction point.

#### 3.4.7 Method 6: Global Polynomial Interpolation (GPI)

GPI fits a single first-order polynomial: Z(x, y) = a₀ + a₁x + a₂y.

### 3.5 Cross-Validation

#### 3.5.1 Leave-One-Out Cross-Validation

Each station is temporarily removed, the interpolation is performed using remaining stations, and the removed value is predicted. This procedure is repeated for all stations.

#### 3.5.2 Accuracy Metrics

The following metrics are used:

1. **RMSE:** √[(1/n) Σ (Zᵢ - Z*ᵢ)²]
2. **MAE:** (1/n) Σ |Zᵢ - Z*ᵢ|
3. **ME:** (1/n) Σ (Z*ᵢ - Zᵢ)
4. **MedAE:** Median of |Zᵢ - Z*ᵢ|
5. **R²:** 1 - (Σ(Zᵢ - Z*ᵢ)²) / (Σ(Zᵢ - Z̄)²)
6. **Regression slope and intercept**

### 3.6 Implementation

#### 3.6.1 ArcGIS Geostatistical Analyst

The kriging and EBK analyses were performed using ArcGIS Geostatistical Analyst. Cross-validation results were exported for statistical analysis and comparison.

#### 3.6.2 Visualization and Presentation of Results

The spatial interpolation results were organized into monthly maps for each method, with consistent color scales to facilitate direct comparison. The results were presented using side-by-side map panels showing all six methods for each month, overlaid with station locations and country boundaries. Regression equations and accuracy metrics were tabulated alongside the maps to provide a comprehensive comparison of the methods.

#### 3.6.3 Workflow Diagram

*[Figure 3.2: Flowchart showing the methodology workflow from data collection through preprocessing, interpolation, cross-validation, and visualization.]*

### 3.7 Summary of Methods

**Table 3.2:** Summary of the six interpolation methods.

| Method | Type | Variogram Required | Exact Interpolator | Uncertainty Estimate |
|--------|------|-------------------|-------------------|---------------------|
| IDW | Deterministic | No | No | No |
| Ordinary Kriging | Geostatistical | Yes | No | Yes |
| EBK | Geostatistical | Yes (automated) | No | Yes |
| RBF | Deterministic | No | Yes | No |
| LPI | Deterministic | No | No | No |
| GPI | Deterministic | No | No | No |
