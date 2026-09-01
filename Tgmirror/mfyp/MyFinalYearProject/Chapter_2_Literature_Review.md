# CHAPTER TWO

## LITERATURE REVIEW

### 2.1 Introduction

This chapter presents a comprehensive review of the literature relevant to the spatial modeling of solar radiation using geostatistical methods. The review is organized into the following sections: an overview of solar radiation and its measurement, a discussion of spatial interpolation methods, an introduction to geostatistical theory and kriging techniques, the theoretical framework for spatial analysis, a review of previous studies on solar radiation modeling, and identification of the research gaps addressed by this study.

### 2.2 Solar Radiation: Concepts and Measurement

#### 2.2.1 Components of Solar Radiation

Solar radiation reaching the Earth's surface consists of several components. The global horizontal irradiance (GHI), also referred to as Surface Incoming Shortwave (SIS) radiation, is the total solar radiation received on a horizontal surface. It is the sum of the direct normal irradiance (DNI), which is the solar radiation received directly from the solar disk, and the diffuse horizontal irradiance (DHI), which is the solar radiation received from the sky after being scattered by the atmosphere (Duffie and Beckman, 2013):

**GHI = DNI × cos(θ) + DHI**

where θ is the solar zenith angle.

The magnitude of solar radiation at any given location is influenced by several factors, including the solar constant (approximately 1,361 W/m² at the top of the atmosphere), the Earth-Sun distance (which varies by ±3.3% over the year), the solar zenith angle (which depends on latitude, time of day, and time of year), and atmospheric attenuation due to clouds, aerosols, water vapor, and ozone (Kasten and Czeplak, 1980).

**Clear-sky solar radiation** represents the maximum solar radiation that would reach the surface in the absence of clouds. It is primarily determined by the solar zenith angle and atmospheric conditions (aerosols, water vapor, ozone). Clear-sky models such as the Ineichen-Perez model (Ineichen and Perez, 2002) and the ESRA model (Rigollier et al., 2000) are widely used to estimate clear-sky irradiance as a baseline for cloud correction algorithms.

**Cloud effects** represent the single largest source of variability in surface solar radiation. Clouds can reduce GHI by 50–80% compared to clear-sky conditions, depending on cloud type, optical thickness, and coverage. The relationship between cloud properties and surface irradiance is complex and non-linear, making cloud correction one of the most challenging aspects of satellite-based solar radiation estimation (Pinker and Laszlo, 1992).

**Aerosol effects** are particularly important in West Africa, where Saharan dust aerosols can significantly attenuate incoming solar radiation. During the Harmattan season (December–February), dust loading over the Sahel can reduce GHI by 10–30% compared to dust-free conditions (Nou et al., 2021). The optical properties of mineral dust aerosols are complex, with both scattering and absorption components that vary with particle size, mineral composition, and humidity.

#### 2.2.2 Solar Radiation Measurement

Ground-based measurement of solar radiation is typically performed using pyranometers, which measure GHI, and pyrheliometers, which measure DNI. The World Meteorological Organization (WMO) classifies pyranometers into classes A, B, and C based on their accuracy and response characteristics (WMO, 2014).

**Class A pyranometers** (secondary standard) have an accuracy of approximately ±2% for daily integrals and are used as reference instruments at high-quality monitoring stations. **Class B pyranometers** (first class) have an accuracy of approximately ±5% and are commonly used at operational weather stations. **Class C pyranometers** (second class) have lower accuracy (±10–15%) and are typically used for screening and monitoring purposes.

The Baseline Surface Radiation Network (BSRN) provides high-quality measurements at approximately 70 stations worldwide, following strict measurement protocols and quality control procedures (Ohmura et al., 1998). However, coverage in Africa is extremely limited, with only a few BSRN stations on the continent. This data scarcity has motivated the development of satellite-based estimation methods and spatial interpolation techniques.

**Data quality considerations** are critical for solar radiation studies. Common data quality issues include:

1. **Instrument degradation:** Pyranometer sensitivity can change over time due to aging, dirt accumulation, and exposure to extreme conditions. Regular calibration against reference instruments is essential.

2. **Shadowing:** Nearby objects (buildings, trees, terrain) can cast shadows on the pyranometer, causing systematic underestimation of solar radiation, particularly during morning and evening hours.

3. **Power failures and data gaps:** Many meteorological stations in developing countries experience frequent power interruptions, leading to gaps in the record.

4. **Systematic biases:** Different pyranometer types and measurement protocols can introduce systematic biases that must be accounted for when combining data from multiple stations.

#### 2.2.3 Satellite-Based Solar Radiation Estimation

Satellite-based methods estimate surface solar radiation by relating cloud properties observed in satellite imagery to surface irradiance. Several satellite-derived SIS datasets are available:

- **CMSAF (Climate Monitoring Satellite Application Facility):** Provides SIS estimates over Europe and Africa based on Meteosat observations using the Heliosat method (Meirink et al., 2013). The CMSAF dataset covers the period from 1983 to present at 0.05° × 0.05° spatial resolution and hourly temporal resolution.

- **NASA POWER (Prediction Of Worldwide Energy Resources):** Offers global solar radiation estimates at 0.5° × 0.5° resolution derived from multiple satellite and reanalysis sources (Stackhouse et al., 2019). The POWER dataset provides monthly, daily, and hourly values for multiple solar radiation components.

- **SARAH (Surface Solar Radiation Data Set – Heliosat):** Provides high-resolution SIS estimates over Europe, Africa, and the Middle East based on the Heliosat method applied to Meteosat data (Müller et al., 2015). SARAH version 2.1 covers the period from 1983 to present at 0.05° × 0.05° resolution.

- **CERES (Clouds and the Earth's Radiant Energy System):** Provides global radiation budgets from satellite observations at 1° × 1° resolution (Wielicki et al., 1996). CERES data are widely used for climate studies and energy budget analysis.

While satellite-derived datasets offer excellent spatial coverage, they are subject to biases and uncertainties, particularly in regions with complex atmospheric conditions such as the Sahel, where dust aerosols and rapidly changing cloud patterns can affect retrieval accuracy (Nou et al., 2021). Ground-based measurements remain essential for calibration and validation of satellite products.

#### 2.2.4 Solar Radiation Variability

Solar radiation exhibits variability at multiple temporal scales:

**Diurnal variation:** Solar radiation follows an approximately sinusoidal pattern during the day, peaking at solar noon and reaching zero at night. The amplitude of the diurnal cycle is influenced by latitude, season, and atmospheric conditions.

**Seasonal variation:** The annual cycle of solar radiation is driven by changes in the solar zenith angle and cloud cover. In West Africa, seasonal variation is particularly pronounced due to the monsoon cycle, with monthly mean SIS varying by 30–50% between the dry and wet seasons.

**Inter-annual variation:** Year-to-year changes in solar radiation are driven by large-scale climate modes such as the El Niño-Southern Oscillation (ENSO), the North Atlantic Oscillation (NAO), and variations in the West African Monsoon. These inter-annual variations can be significant, with annual mean SIS varying by 5–10% between years.

**Secular trends:** Long-term changes in solar radiation, often referred to as "global dimming" and "brightening," have been observed at many locations worldwide. Wild et al. (2005) documented a decline in surface solar radiation of approximately 4% per decade from the 1960s to the 1990s (dimming), followed by a recovery of approximately 2% per decade from the 1990s to the 2000s (brightening). These trends are attributed to changes in aerosol loading and cloud cover.

### 2.3 Spatial Interpolation Methods

Spatial interpolation is the process of estimating values of a variable at unsampled locations based on observations at nearby sample points. The choice of interpolation method depends on the nature of the spatial variation, the density and distribution of sampling points, and the specific requirements of the application (Webster and Oliver, 2007).

#### 2.3.1 Deterministic Methods

**Inverse Distance Weighting (IDW):**

IDW is one of the simplest and most widely used interpolation methods. It estimates the value at an unsampled point as a weighted average of the values at surrounding sample points, where the weights are inversely proportional to the distance between the sample and prediction points raised to a power p (Shepard, 1968):

**Z(x₀) = Σᵢ wᵢ Z(xᵢ) / Σᵢ wᵢ**

where **wᵢ = 1/d(x₀, xᵢ)ᵖ**, d is the Euclidean distance, and p is the power parameter (typically p = 2).

IDW assumes that the variable being interpolated is influenced more by nearby points than by distant ones, and that the influence decreases smoothly with distance. The method is computationally simple and requires no assumption about the statistical properties of the data. However, IDW has several limitations:

1. It does not provide any measure of prediction uncertainty.
2. It can produce artifacts (such as bull's-eye patterns) around sample points, particularly when the power parameter is high.
3. It does not account for directional trends or anisotropy in the data.
4. The predicted values are always bounded within the range of the observed values, which may not be appropriate for variables with strong trends.

The choice of the power parameter p significantly affects the interpolation results. Lower values of p produce smoother surfaces, while higher values produce more localized predictions. Watson and Philip (1985) recommended p = 2 as a reasonable default, but noted that the optimal value depends on the spatial characteristics of the data.

**Radial Basis Function (RBF) Interpolation:**

RBF methods fit a smooth surface that passes exactly through (or close to) the sample points by summing the contributions of radial basis functions centered at each sample point (Franke, 1982):

**Z(x) = Σᵢ λᵢ φ(‖x - xᵢ‖) + p(x)**

where φ is the radial basis function, λᵢ are coefficients, and p(x) is a polynomial trend. Common radial basis functions include:

- **Thin-plate spline:** φ(r) = r² ln(r) — the most commonly used RBF for spatial interpolation
- **Multiquadric:** φ(r) = √(r² + δ²) — where δ is a shape parameter
- **Gaussian:** φ(r) = exp(-r²/δ²) — produces very smooth surfaces

RBF methods are exact interpolators (they reproduce the observed values at sample points) and can model complex spatial patterns. The thin-plate spline is particularly well-suited for modeling smooth, continuous surfaces such as solar radiation, which varies gradually across space (Hastie et al., 2009).

However, RBF methods may not perform well when the data contains measurement errors or when extrapolation beyond the range of sample points is required. The exact interpolation property means that measurement errors are reproduced in the interpolated surface, which can lead to artifacts in noisy data.

**Polynomial Interpolation:**

Global Polynomial Interpolation (GPI) fits a single polynomial surface to the entire dataset, capturing large-scale trends in the data. The simplest form is a first-order (linear) polynomial:

**Z(x, y) = a₀ + a₁x + a₂y**

Higher-order polynomials can capture more complex trends but are prone to overfitting and can produce unrealistic oscillations at the edges of the domain (Runge's phenomenon).

Local Polynomial Interpolation (LPI) fits polynomials within a moving window centered at each prediction point, allowing it to capture both local and global trends. The polynomial order and window size are key parameters that control the balance between smoothing and detail. LPI is more flexible than GPI but can be sensitive to the choice of these parameters (Watson, 1984).

#### 2.3.2 Geostatistical Methods

Geostatistics provides a framework for spatial interpolation that explicitly accounts for the spatial correlation structure of the data. Unlike deterministic methods, geostatistical methods provide not only predictions but also estimates of prediction uncertainty, which are essential for risk assessment and decision-making (Cressie, 1993).

**The Variogram:**

The fundamental tool in geostatistics is the variogram (or semivariogram), which quantifies the spatial dependence between observations as a function of the distance and direction separating them. The empirical variogram is calculated as:

**γ(h) = (1/2N(h)) Σ [Z(xᵢ) - Z(xᵢ + h)]²**

where N(h) is the number of pairs of points separated by the lag distance h.

The variogram typically increases from zero at h = 0 to a plateau called the sill, which represents the total variance of the data. The distance at which the sill is reached is called the range, and represents the maximum distance over which spatial correlation exists. The nugget effect, a discontinuity at the origin, represents measurement error or micro-scale variability (Webster and Oliver, 2007).

**Variogram Models:**

Several mathematical models are available for fitting the empirical variogram:

1. **Spherical model:** γ(h) = c₀ + c[1.5(h/a) - 0.5(h/a)³] for h ≤ a; γ(h) = c₀ + c for h > a. This model reaches the sill at a finite range a and is commonly used for variables with well-defined spatial correlation ranges.

2. **Exponential model:** γ(h) = c₀ + c[1 - exp(-h/a)]. This model approaches the sill asymptotally and is often used as a default model in geostatistical analysis.

3. **Gaussian model:** γ(h) = c₀ + c[1 - exp(-h²/a²)]. This model produces very smooth interpolated surfaces and is suitable for variables that vary smoothly over space.

4. **Matérn model:** A flexible model that includes a smoothness parameter ν, allowing control over the differentiability of the interpolated surface.

where c₀ is the nugget, c is the partial sill, and a is the range parameter.

**Ordinary Kriging:**

Ordinary Kriging (OK) is the most commonly used kriging method. It assumes that the mean of the random field is unknown but constant within the local neighborhood of the prediction point. The kriging estimator is a weighted linear combination of the observed values:

**Z*(x₀) = Σᵢ λᵢ Z(xᵢ)**

where the weights λᵢ are determined by minimizing the kriging variance subject to the unbiasedness constraint Σλᵢ = 1 (Matheron, 1963).

The kriging weights depend on the variogram model and the geometric configuration of the sample points relative to the prediction point. Points that are close together and far from other points receive higher weights, ensuring that the interpolation accounts for both proximity and redundancy in the data.

The kriging variance provides a measure of prediction uncertainty:

**σ²(x₀) = Σᵢ λᵢ γ(x₀, xᵢ) + μ**

where μ is the Lagrange multiplier from the optimization. The kriging variance is smallest near sample points and increases with distance from the samples, reflecting the increasing uncertainty of predictions in data-sparse areas.

**Simple Kriging (SK):**

Simple Kriging assumes that the mean of the random field is known and constant across the study area. This assumption is rarely met in practice, but SK can be useful when a reliable estimate of the mean is available (e.g., from a long-term average). The SK estimator is:

**Z*(x₀) = m + Σᵢ λᵢ [Z(xᵢ) - m]**

where m is the known mean. SK produces lower kriging variances than OK when the mean is correctly specified, but can be biased when the assumed mean is incorrect.

**Universal Kriging (UK):**

Universal Kriging extends Ordinary Kriging by allowing the mean to vary according to a polynomial trend:

**m(x) = Σₖ βₖ fₖ(x)**

where fₖ(x) are known functions (typically polynomials of the coordinates) and βₖ are unknown coefficients. UK is appropriate when the data exhibit a systematic spatial trend, such as the latitudinal gradient in solar radiation (Cressie, 1993). In West Africa, the strong north-south gradient in solar radiation could potentially be captured by UK, though in this study we focus on OK and EBK.

**Empirical Bayes Kriging (EBK):**

Empirical Bayes Kriging is a more recent development in geostatistics that addresses some of the limitations of traditional kriging methods. EBK differs from Ordinary Kriging in several important ways (Krivoruchko, 2012):

1. **Log-normal transformation:** EBK assumes that the data follow a log-normal distribution and applies a log-transformation before performing kriging. This is particularly appropriate for variables such as solar radiation, which are strictly positive and often exhibit right-skewed distributions.

2. **Automated variogram fitting:** EBK uses an automated process to fit the variogram model, reducing the subjectivity associated with manual variogram interpretation.

3. **Back-transformation:** After kriging in the log-transformed space, the predictions are back-transformed using the exponential function, with a correction for the bias introduced by the non-linear transformation:

**Z*(x₀) = exp[y*(x₀) + σ²(x₀)/2]**

4. **Local parameter estimation:** EBK estimates the variogram parameters locally, allowing the spatial correlation structure to vary across the study area.

The combination of these features makes EBK well-suited for modeling solar radiation, which often exhibits non-Gaussian distributions, non-stationary spatial patterns, and varying degrees of spatial correlation across different climatic zones.

#### 2.3.3 Comparison of Methods

**Table 2.1:** Comparison of interpolation method characteristics.

| Method | Type | Complexity | Uncertainty Estimate | Handles Trends | Handles Anisotropy |
|--------|------|-----------|---------------------|----------------|-------------------|
| IDW | Deterministic | Low | No | No | No |
| Ordinary Kriging | Geostatistical | High | Yes | No | Yes |
| EBK | Geostatistical | High | Yes | Partial | Yes |
| RBF | Deterministic | Medium | No | Partial | No |
| LPI | Deterministic | Medium | No | Yes | No |
| GPI | Deterministic | Low | No | Yes | No |

### 2.4 Theoretical Framework for Spatial Analysis

#### 2.4.1 Stationarity Assumptions

The validity of geostatistical methods depends on the stationarity assumptions underlying the random field model. Three levels of stationarity are recognized:

**Strict stationarity:** The joint probability distribution of the random field is invariant under translation. This is the strongest assumption and is rarely met in practice for environmental variables.

**Second-order (weak) stationarity:** The mean is constant and the covariance depends only on the lag vector, not on the location. This is the most commonly assumed form of stationarity in geostatistical practice:

**E[Z(x)] = m (constant)**
**Cov[Z(x), Z(x+h)] = C(h)**

**Intrinsic stationarity:** The mean is constant and the variogram exists and depends only on the lag vector. This is a weaker assumption than second-order stationarity and is sufficient for ordinary kriging:

**E[Z(x)] = m**
**Var[Z(x) - Z(x+h)] = 2γ(h)**

For solar radiation in West Africa, strict stationarity is clearly violated due to the strong north-south gradient and seasonal variation. However, within local neighborhoods, the intrinsic stationarity assumption may be approximately valid, particularly for monthly climatological data where the temporal smoothing reduces non-stationarity.

#### 2.4.2 Anisotropy

Anisotropy refers to the dependence of spatial correlation on direction. In solar radiation data, anisotropy may arise from:

1. **Geometric anisotropy:** The range of spatial correlation varies with direction but the sill remains constant. This could occur if solar radiation is more spatially correlated in the east-west direction (along latitude lines) than in the north-south direction (across climate zones).

2. **Zonal anisotropy:** Both the range and sill vary with direction. This is a more complex form of anisotropy that may be present in regions with strong directional trends.

In this study, we use isotropic variograms (assuming the same spatial correlation in all directions) for simplicity. However, future studies could explore the use of anisotropic variograms to better capture the directional structure of solar radiation variability.

#### 2.4.3 Cross-Validation Theory

Cross-validation is based on the principle that a good interpolation model should be able to predict held-out observations accurately. The leave-one-out cross-validation (LOOCV) procedure produces n prediction errors (one for each observation), which can be used to estimate the distribution of prediction errors.

The key assumption of cross-validation is that the prediction errors are representative of the errors that would be obtained for new, unseen locations. This assumption is valid when the spatial process is stationary and the observations are independent given the spatial correlation structure.

LOOCV has several advantages over other validation approaches:

1. **Maximum use of data:** Every observation is used for both training and validation.
2. **Unbiased estimation:** The cross-validation estimator is approximately unbiased for the true prediction error.
3. **Model comparison:** Different methods can be compared on the same validation set.

However, LOOCV can be computationally expensive for large datasets (n cross-validation iterations), and the prediction errors are not independent (since the training sets overlap).

### 2.5 Cross-Validation in Geostatistics

Cross-validation is an essential tool for assessing the accuracy and reliability of spatial interpolation models. In the geostatistical context, leave-one-out cross-validation (LOOCV) is commonly used: each observation is temporarily removed from the dataset, the model is re-fitted using the remaining observations, and the removed value is predicted (Hastie et al., 2009).

Several accuracy metrics are commonly used to evaluate cross-validation results:

- **Root Mean Square Error (RMSE):** Provides a measure of the average magnitude of prediction errors:

**RMSE = √[(1/n) Σ (Zᵢ - Z*ᵢ)²]**

- **Mean Absolute Error (MAE):** Provides a measure of the average absolute deviation of predictions from observations:

**MAE = (1/n) Σ |Zᵢ - Z*ᵢ|**

- **Mean Error (ME):** Measures the bias of the predictions:

**ME = (1/n) Σ (Z*ᵢ - Zᵢ)**

- **Coefficient of Determination (R²):** Measures the proportion of variance in the observed values that is explained by the predictions. R² values close to 1.0 indicate excellent agreement.

- **Regression parameters:** The slope and intercept of the linear regression of predicted versus observed values provide a measure of systematic bias. An ideal model would produce a slope of 1.0 and an intercept of 0.0.

### 2.6 Previous Studies on Solar Radiation Interpolation

#### 2.6.1 Global Studies

Numerous studies have investigated the spatial interpolation of solar radiation using various methods. These studies provide a rich body of evidence on the relative performance of different methods in different settings.

**Mubiru et al. (2008)** compared IDW, Kriging, and artificial neural networks for estimating GHI across Uganda. The study found that neural networks outperformed traditional interpolation methods in areas with complex terrain, but that Kriging provided the most reliable estimates in flat areas with adequate station coverage.

**Jang et al. (2011)** evaluated the performance of Kriging, IDW, and RBF for mapping solar radiation in South Korea. The study reported that Ordinary Kriging generally produced the most accurate results when the variogram was properly fitted, but that RBF performed comparably in some situations.

**Li et al. (2013)** developed a hybrid approach combining satellite-derived radiation estimates with ground-based Kriging interpolation for mapping solar radiation across China. The study achieved improved accuracy over either method alone.

**Almorox and Hontoria (2004)** applied multiple interpolation methods to estimate solar radiation across Spain, comparing IDW, Kriging, and angular distance weighting. The study found that Kriging with an exponential variogram model produced the most accurate results.

**Bahel et al. (2005)** compared several interpolation methods for estimating solar radiation in Iran, reporting that the choice of method had a significant impact on the accuracy of spatial estimates.

**Badescu (2002)** reviewed various methods for estimating solar radiation, including empirical models, satellite-based methods, and spatial interpolation techniques. The study concluded that no single method is universally superior.

**Paulescu et al. (2013)** compared several interpolation methods for estimating solar radiation across Romania, finding that Kriging and RBF produced the most accurate results.

**Tymvios et al. (2005)** compared angular distance weighting with Kriging and IDW for mapping solar radiation across Cyprus, finding that angular distance weighting performed comparably to Kriging in some situations.

#### 2.6.2 African Studies

**Younes et al. (2005)** reviewed methods for estimating solar radiation in data-sparse regions and highlighted the importance of selecting appropriate interpolation methods for African climates.

**Houndeganji et al. (2015)** applied geostatistical methods to map solar radiation across Benin, comparing Ordinary Kriging with IDW. The study found that Kriging produced superior spatial models.

**Akpinar et al. (2016)** evaluated multiple interpolation methods for estimating solar radiation across Turkey, finding that Kriging with an exponential variogram model provided the most reliable estimates.

**Malgwi et al. (2020)** applied spatial analysis techniques to assess solar energy potential across Nigeria, demonstrating the applicability of geostatistical methods for solar resource assessment in the West African context.

**Nou et al. (2021)** compared satellite-derived SIS estimates with ground measurements across the Sahel, finding significant biases in satellite products during dusty conditions.

**Sanusi et al. (2020)** evaluated the performance of different interpolation methods for mapping solar radiation across Nigeria, comparing IDW, Kriging, and spline methods.

**Olatunji et al. (2021)** applied geostatistical methods to assess solar energy potential in southwestern Nigeria, comparing multiple kriging variants and deterministic methods.

**Adeniyi (2019)** assessed solar energy potential across Nigeria using spatial interpolation techniques, finding that the southern regions receive less solar radiation due to cloud cover.

**Akinbulumo et al. (2020)** compared different interpolation methods for estimating solar radiation across Ogun State, Nigeria, finding that Kriging outperformed IDW and RBF.

**Ogolo et al. (2021)** assessed solar energy potential in the northern Nigerian savanna using geostatistical methods, finding that Ordinary Kriging with an exponential variogram model produced the most accurate spatial estimates.

#### 2.6.3 Methodological Studies

**Krivoruchko (2012)** provided a comprehensive review of Empirical Bayes Kriging, demonstrating its advantages over traditional kriging for modeling non-Gaussian, non-stationary environmental variables.

**Oliver and Webster (2014)** reviewed the history and development of geostatistics, emphasizing the importance of proper variogram modeling and cross-validation.

**Chiles and Delfiner (2012)** provided an authoritative treatment of geostatistical theory, including advanced topics such as non-stationary kriging and Bayesian approaches.

**Webster and Oliver (2007)** provided a practical guide to geostatistical methods for environmental scientists, covering the selection of sampling strategies, variogram modeling, kriging, and cross-validation.

**Cressie (1993)** provided a foundational treatment of spatial statistics, including geostatistics, point process models, and lattice models.

**Goovaerts (1997)** provided a comprehensive treatment of geostatistics for natural resource assessment, including detailed discussions of kriging variants, non-linear geostatistics, and spatial uncertainty modeling.

**Wackernagel (2003)** provided an accessible introduction to multivariate geostatistics, including co-kriging and other methods for incorporating auxiliary variables into spatial interpolation.

### 2.7 Research Gap

While the existing literature demonstrates the applicability of geostatistical methods for solar radiation estimation, several gaps remain:

1. **Limited comparative studies in West Africa:** Most studies have focused on individual methods or limited comparisons within specific countries. A comprehensive evaluation of multiple geostatistical methods specifically for the West African region, spanning multiple climatic zones, is lacking.

2. **EBK for solar radiation:** Empirical Bayes Kriging has been applied to various environmental variables but has received limited attention for solar radiation modeling in the tropics.

3. **Long-term climatological analysis:** Few studies have applied geostatistical methods to long-term (multi-decadal) solar radiation records across West Africa.

4. **Operational tools:** There is a need for practical, accessible tools that allow non-specialists to compare and evaluate different interpolation methods for solar radiation.

This study addresses these gaps by providing a systematic comparison of six interpolation methods—including EBK—for SIS across West Africa, using a 40-year climatological record and ArcGIS-based cross-validation, with results presented through comparative monthly maps and statistical tables.

### 2.8 Summary of Literature Review

The literature review has established that solar radiation is a critical variable for energy, agriculture, and climate applications, and that its spatial modeling is essential for filling gaps in ground-based observations. Geostatistical methods, particularly Kriging and its variants, provide a rigorous framework for spatial interpolation with the added benefit of uncertainty quantification. The development of EBK offers improvements for modeling non-Gaussian and non-stationary data, making it potentially well-suited for solar radiation in the diverse climatic zones of West Africa. Cross-validation techniques provide an objective means of evaluating and comparing different interpolation methods. This study builds on this foundation by applying and comparing six methods for SIS interpolation across West Africa, using ArcGIS-based cross-validation, with results presented through comparative monthly maps and accuracy tables.
