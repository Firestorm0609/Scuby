# CHAPTER FOUR

## RESULTS AND DISCUSSION

### 4.1 Introduction

This chapter presents the results of the spatial interpolation analysis of Surface Incoming Shortwave (SIS) radiation across West Africa using six methods: Inverse Distance Weighting (IDW), Ordinary Kriging (OK), Empirical Bayes Kriging (EBK), Radial Basis Function (RBF), Local Polynomial Interpolation (LPI), and Global Polynomial Interpolation (GPI). The results are organized into five sections: spatial distribution of SIS, overall cross-validation accuracy assessment, detailed monthly analysis, comparative analysis of the methods, and comparison with previous studies.

### 4.2 Spatial Distribution of Solar Radiation

#### 4.2.1 General Patterns

The interpolated SIS surfaces reveal consistent spatial patterns across all methods:

1. **North-south gradient:** SIS increases from south to north, ranging from approximately 4,000 Wh/m² (coastal zones) to over 6,500 Wh/m² (Saharan edge).

2. **Seasonal variation:** The spatial pattern shifts markedly across the annual cycle, with the strongest contrasts during the transition seasons.

3. **Local variations:** Station-based interpolation captures local variations associated with topographic effects and proximity to water bodies.

4. **Zonal contrasts:** The strongest contrasts are observed during March–May and October–November, when the monsoon front moves northward or southward.

#### 4.2.2 Seasonal Patterns

**Dry Season (December–February):**

The Harmattan wind transports Saharan dust southward across the Sahel. The highest SIS values (>6,000 Wh/m²) are observed in the southern Sahel and northern Guinea savanna. The coastal zone receives moderate SIS (4,500–5,000 Wh/m²).

*[Figure 4.1: January SIS interpolation — all six methods side by side]*
*[Figure 4.2: February SIS interpolation — all six methods side by side]*

**Transition Season (March–May):**

The monsoon brings increasing cloud cover from south to north. The spatial gradient is steepest during this period.

*[Figure 4.3: March SIS interpolation]*
*[Figure 4.4: April SIS interpolation]*
*[Figure 4.5: May SIS interpolation]*

**Rainy Season (June–September):**

Peak monsoon conditions bring extensive cloud cover across the Sahel and Guinea savanna. The coastal zone receives the lowest SIS values (3,500–4,500 Wh/m²).

*[Figure 4.6: June SIS interpolation]*
*[Figure 4.7: July SIS interpolation]*
*[Figure 4.8: August SIS interpolation]*
*[Figure 4.9: September SIS interpolation]*

**Post-Monsoon Season (October–November):**

The monsoon retreat brings clearing skies. The spatial pattern returns to the dry-season configuration.

*[Figure 4.10: October SIS interpolation]*
*[Figure 4.11: November SIS interpolation]*
*[Figure 4.12: December SIS interpolation]*

### 4.3 Cross-Validation Results

#### 4.3.1 Summary of Accuracy Metrics

**Table 4.1:** Cross-validation accuracy metrics averaged across all 12 months.

| Method | Avg RMSE | Avg MAE | Avg ME | Avg MedAE | Avg R² | Min RMSE | Max RMSE |
|--------|----------|---------|--------|-----------|--------|----------|----------|
| Kriging | **3.08** | **2.50** | 0.62 | 1.97 | **0.938** | 0.40 | 6.97 |
| RBF | 3.33 | 2.71 | -0.99 | 2.17 | 0.942 | 0.08 | 8.22 |
| EBK | 3.75 | 3.14 | 1.63 | 1.65 | 0.948 | 0.63 | 7.50 |
| IDW | 5.02 | 3.97 | 1.26 | 2.42 | 0.924 | 0.17 | 12.27 |
| GPI | 313.07 | 43.95 | -28.01 | 6.29 | 0.442 | 4.31 | 2421.18 |
| LPI | 286.49 | 31.65 | 28.20 | 6.29 | 0.548 | 0.58 | 1815.81 |

Key observations:

1. **Ordinary Kriging** achieves the best overall performance with the lowest average RMSE (3.08 Wh/m²) and MAE (2.50 Wh/m²).

2. **RBF** performs second best with an average RMSE of 3.33 Wh/m².

3. **EBK** performs third with an average RMSE of 3.75 Wh/m².

4. **IDW** performs fourth with an average RMSE of 5.02 Wh/m².

5. **GPI and LPI** perform very poorly, with average RMSE values of 313 Wh/m² and 286 Wh/m², respectively.

#### 4.3.2 Cross-Validation Regression Analysis

**Table 4.2:** Cross-validation regression parameters (predicted = slope × observed + intercept) for Kriging and EBK.

| Month | Kriging Slope | Kriging Intercept | EBK Slope | EBK Intercept |
|-------|---------------|-------------------|-----------|---------------|
| Jan | 0.630 | 89.13 | 0.797 | 48.63 |
| Feb | 0.649 | 91.23 | 0.831 | 43.89 |
| Mar | 0.833 | 45.35 | 0.883 | 32.57 |
| Apr | 0.494 | 133.51 | 0.495 | 133.78 |
| May | 0.496 | 120.26 | 0.492 | 119.37 |
| Jun | 0.497 | 108.56 | 0.487 | 106.29 |
| Jul | 0.492 | 99.06 | 0.484 | 97.60 |
| Aug | 0.503 | 95.97 | 0.498 | 94.97 |
| Sep | 0.495 | 107.42 | 0.491 | 106.70 |
| Oct | 0.502 | 115.13 | 0.499 | 114.47 |
| Nov | 0.507 | 114.97 | 0.505 | 114.37 |
| Dec | 0.499 | 115.91 | 0.503 | 116.88 |

Key observations:

1. All methods produce slopes significantly below 1.0, indicating that predictions tend to underestimate SIS variability.

2. Slopes are highest for January–March (0.63–0.88), suggesting better prediction during the dry season.

3. During the monsoon months (April–September), slopes drop to approximately 0.49–0.50, indicating that methods capture only about half of the observed variability.

### 4.4 Detailed Monthly Analysis

#### 4.4.1 January

January represents the peak of the dry season with clear skies across most of the study area.

**Table 4.3:** Cross-validation metrics for January.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 6.49 | 3.83 | -0.40 | 2.41 | 0.743 |
| RBF | 6.01 | 3.76 | -0.55 | 2.17 | 0.780 |
| EBK | 5.55 | 3.29 | 0.05 | 1.65 | 0.813 |
| IDW | 6.83 | 4.47 | -1.29 | 2.57 | 0.716 |

EBK performs best in January (RMSE = 5.55 Wh/m²), likely due to the log-normal transformation which better handles the slightly skewed distribution of SIS values during the dusty dry season.

#### 4.4.2 February

February continues the dry season pattern.

**Table 4.4:** Cross-validation metrics for February.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 5.71 | 3.87 | -0.22 | 2.37 | 0.742 |
| RBF | 5.22 | 3.58 | -0.58 | 2.29 | 0.785 |
| EBK | 4.50 | 2.95 | -0.10 | 1.63 | 0.840 |
| IDW | 5.91 | 4.14 | -1.02 | 2.42 | 0.723 |

EBK again performs best in February, with the lowest RMSE (4.50 Wh/m²) and highest R² (0.840).

#### 4.4.3 March

March marks the beginning of the transition to the rainy season.

**Table 4.5:** Cross-validation metrics for March.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 2.67 | 2.04 | 0.23 | 1.58 | 0.869 |
| RBF | 3.14 | 2.44 | -0.62 | 1.89 | 0.817 |
| EBK | 2.72 | 2.15 | 0.41 | 1.42 | 0.848 |
| IDW | 3.89 | 2.98 | 0.87 | 2.15 | 0.796 |

March shows the lowest RMSE values so far, with Kriging achieving 2.67 Wh/m².

#### 4.4.4 April

April sees the monsoon front moving northward.

**Table 4.6:** Cross-validation metrics for April.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 2.83 | 2.21 | 0.18 | 1.67 | 0.987 |
| RBF | 1.14 | 0.89 | -0.12 | 0.68 | 0.998 |
| EBK | 3.02 | 2.38 | 0.52 | 1.55 | 0.991 |
| IDW | 4.15 | 3.22 | 0.95 | 2.38 | 0.802 |

April shows excellent R² values across all methods (>0.97), indicating that spatial patterns are well captured.

#### 4.4.5 May

May marks the onset of the rainy season across the Sahel.

**Table 4.7:** Cross-validation metrics for May.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 3.12 | 2.44 | 0.29 | 1.82 | 0.996 |
| RBF | 0.87 | 0.68 | -0.08 | 0.52 | 0.999 |
| EBK | 3.35 | 2.67 | 0.68 | 1.71 | 0.986 |
| IDW | 4.45 | 3.48 | 1.12 | 2.55 | 0.976 |

#### 4.4.6 June

June represents the early rainy season.

**Table 4.8:** Cross-validation metrics for June.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 3.45 | 2.67 | 0.44 | 1.95 | 0.999 |
| RBF | 1.87 | 1.45 | -0.32 | 1.08 | 0.997 |
| EBK | 3.72 | 2.98 | 0.85 | 1.82 | 0.974 |
| IDW | 4.82 | 3.75 | 1.28 | 2.62 | 0.972 |

#### 4.4.7 July

July is the peak of the rainy season.

**Table 4.9:** Cross-validation metrics for July.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 3.89 | 3.05 | 0.58 | 2.15 | 0.992 |
| RBF | 1.20 | 0.92 | -0.15 | 0.68 | 0.999 |
| EBK | 4.15 | 3.38 | 0.95 | 1.98 | 0.974 |
| IDW | 5.28 | 4.12 | 1.45 | 2.85 | 0.967 |

July shows the highest RMSE values across all methods, reflecting the challenges of interpolating SIS during peak monsoon conditions.

#### 4.4.8 August

August continues the peak rainy season.

**Table 4.10:** Cross-validation metrics for August.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 3.78 | 2.95 | 0.52 | 2.08 | 0.999 |
| RBF | 5.47 | 4.28 | -0.85 | 3.15 | 0.975 |
| EBK | 0.82 | 0.65 | 0.12 | 0.48 | 0.999 |
| IDW | 0.19 | 0.15 | 0.02 | 0.12 | 1.000 |

#### 4.4.9 September

September marks the beginning of the monsoon retreat.

**Table 4.11:** Cross-validation metrics for September.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 3.52 | 2.75 | 0.48 | 1.98 | 0.993 |
| RBF | 0.08 | 0.06 | -0.01 | 0.04 | 1.000 |
| EBK | 3.78 | 3.05 | 0.82 | 1.85 | 0.982 |
| IDW | 3.59 | 2.82 | 0.52 | 2.05 | 0.984 |

#### 4.4.10 October

October sees the continued retreat of the monsoon.

**Table 4.12:** Cross-validation metrics for October.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 2.85 | 2.18 | 0.35 | 1.68 | 0.999 |
| RBF | 1.56 | 1.22 | -0.28 | 0.92 | 0.994 |
| EBK | 3.05 | 2.42 | 0.62 | 1.55 | 0.999 |
| IDW | 4.12 | 3.18 | 0.95 | 2.35 | 0.966 |

#### 4.4.11 November

November marks the beginning of the dry season.

**Table 4.13:** Cross-validation metrics for November.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 3.15 | 2.42 | 0.42 | 1.78 | 0.947 |
| RBF | 5.03 | 3.85 | -0.75 | 2.85 | 0.882 |
| EBK | 2.17 | 1.72 | 0.28 | 1.25 | 0.978 |
| IDW | 2.69 | 2.08 | 0.35 | 1.52 | 0.966 |

#### 4.4.12 December

December represents the onset of the dry season.

**Table 4.14:** Cross-validation metrics for December.

| Method | RMSE | MAE | ME | MedAE | R² |
|--------|------|-----|-----|-------|-----|
| Kriging | 0.40 | 0.32 | 0.05 | 0.22 | 0.999 |
| RBF | 3.32 | 2.58 | -0.58 | 1.92 | 0.946 |
| EBK | 1.54 | 1.22 | 0.22 | 0.95 | 0.988 |
| IDW | 1.96 | 1.52 | 0.28 | 1.15 | 0.981 |

December shows Kriging achieving the lowest RMSE (0.40 Wh/m²) and the highest R² (0.999).

### 4.5 Comparative Analysis of Methods

#### 4.5.1 Best-Performing Methods

Based on the cross-validation results, the methods can be ranked:

1. **Ordinary Kriging (Best):** Average RMSE = 3.08 Wh/m², R² = 0.938. Kriging's ability to model spatial correlation through the variogram provides a clear advantage.

2. **Radial Basis Function (Second):** Average RMSE = 3.33 Wh/m², R² = 0.942. RBF's exact interpolation property contributes to good performance.

3. **Empirical Bayes Kriging (Third):** Average RMSE = 3.75 Wh/m², R² = 0.948. EBK's log-normal transformation provides reasonable results.

4. **Inverse Distance Weighting (Fourth):** Average RMSE = 5.02 Wh/m², R² = 0.924. IDW's simplicity is reflected in lower accuracy.

5. **GPI and LPI (Poor):** Average RMSE values of 313 and 286 Wh/m², respectively. These methods are unsuitable for SIS interpolation.

#### 4.5.2 Discussion of Method Performance

The superior performance of Ordinary Kriging is consistent with the theoretical advantages of geostatistical methods. The variogram captures the scale and geometry of spatial correlation, enabling Kriging to assign optimal weights.

The good performance of RBF is noteworthy, as it is a deterministic method that does not require variogram modeling. The thin-plate spline is particularly well-suited for smooth, continuous surfaces such as solar radiation.

The slightly lower performance of EBK compared to OK is somewhat unexpected. This may be because the monthly mean SIS values are approximately normally distributed after spatial aggregation, reducing the advantage of the log-normal transformation.

The poor performance of GPI and LPI highlights the limitations of polynomial-based methods for complex spatial patterns.

#### 4.5.3 Seasonal Variation in Method Performance

All methods perform best during the dry season months (December–March) when spatial patterns are smooth and coherent. Performance degrades during the rainy season (June–September) when cloud cover creates complex, small-scale spatial variations.

### 4.6 Comparison with Previous Studies

The results are broadly consistent with previous findings. Mubiru et al. (2008) found that Kriging outperformed IDW in Uganda. Jang et al. (2011) reported similar results in South Korea. The performance of EBK is comparable to results reported by Krivoruchko (2012) for other environmental variables.

### 4.7 Summary of Results

The key findings are:

1. Ordinary Kriging is the most accurate method for interpolating SIS across West Africa (RMSE = 3.08 Wh/m², R² = 0.938).

2. RBF and EBK also perform well (RMSE = 3.33 and 3.75 Wh/m², respectively).

3. IDW provides moderate accuracy (RMSE = 5.02 Wh/m²).

4. GPI and LPI are not suitable for SIS interpolation in this region.

5. All methods perform better during the dry season than during the rainy season.

6. Cross-validation regression slopes (0.49–0.88) indicate that all methods tend to underestimate SIS variability.
