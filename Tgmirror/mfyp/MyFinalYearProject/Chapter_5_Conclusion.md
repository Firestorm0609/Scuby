# CHAPTER FIVE

## CONCLUSION AND RECOMMENDATIONS

### 5.1 Summary of the Study

This study investigated the spatial modeling of Surface Incoming Shortwave (SIS) radiation across West Africa using geostatistical analysis techniques. The research was motivated by the need for accurate, spatially continuous solar radiation estimates in a region characterized by sparse ground-based measurement networks and significant potential for solar energy development.

The study employed six spatial interpolation methods—Inverse Distance Weighting (IDW), Ordinary Kriging (OK), Empirical Bayes Kriging (EBK), Radial Basis Function (RBF), Local Polynomial Interpolation (LPI), and Global Polynomial Interpolation (GPI)—to generate monthly mean SIS surfaces from 108 meteorological stations across the study area, covering the period 1983–2022. The accuracy of each method was evaluated through leave-one-out cross-validation, and the results were presented through comparative monthly maps and statistical tables.

The key findings of the study are summarized as follows:

1. **Ordinary Kriging** is the most accurate method for interpolating SIS across West Africa, achieving the lowest average cross-validation RMSE of 3.08 Wh/m² and MAE of 2.50 Wh/m², with an average R² of 0.938.

2. **RBF and EBK** also perform well, with average RMSE values of 3.33 and 3.75 Wh/m², respectively, and R² values of 0.942 and 0.948.

3. **IDW** provides moderate accuracy (average RMSE = 5.02 Wh/m², R² = 0.924) but is outperformed by the more sophisticated methods.

4. **GPI and LPI** are not suitable for SIS interpolation in this region, producing errors that are orders of magnitude larger than the other methods.

5. **Seasonal variation in accuracy** is a consistent feature across all methods, with better performance during the dry season and reduced accuracy during the rainy season.

### 5.2 Key Findings

The following key findings emerged from this study:

#### 5.2.1 Method Performance

The cross-validation analysis revealed that Ordinary Kriging consistently outperformed the other methods across most months, with the lowest RMSE and MAE values. The ability of Kriging to model the spatial correlation structure through the variogram provides a clear advantage over deterministic methods. The variogram captures the scale and geometry of spatial correlation, enabling Kriging to assign optimal weights that balance proximity and redundancy among stations.

RBF achieved the second-best performance, with an average RMSE of 3.33 Wh/m². As an exact interpolator, RBF produces predictions that pass through the observed values at station locations, which contributes to its good cross-validation performance. The thin-plate spline RBF is particularly well-suited for modeling smooth, continuous surfaces such as solar radiation.

EBK achieved an average RMSE of 3.75 Wh/m², slightly higher than OK and RBF. The log-normal transformation and automated variogram fitting provided reasonable results, though the performance was marginally below that of OK. This may be because the monthly mean SIS values are approximately normally distributed after the spatial aggregation, reducing the advantage of the log-normal transformation used in EBK.

IDW achieved an average RMSE of 5.02 Wh/m², which is notably higher than the top three methods. The simplicity of IDW—using only distance-weighted averages without modeling spatial correlation—is reflected in its lower accuracy.

GPI and LPI performed very poorly, with average RMSE values of 313 Wh/m² and 286 Wh/m², respectively. These methods are clearly unsuitable for SIS interpolation in this region, as they cannot capture the complex spatial patterns of solar radiation across diverse climatic zones.

#### 5.2.2 Seasonal Variation

All methods exhibit significant seasonal variation in performance, with better accuracy during the dry season (December–March) and reduced accuracy during the rainy season (June–September). This reflects the challenges of interpolating SIS in the presence of complex, small-scale cloud patterns that characterize the monsoon season.

The cross-validation regression slopes ranged from 0.49 to 0.88, indicating that all methods tend to underestimate the variability of SIS. The slopes were highest during the dry season (0.63–0.88) and lowest during the rainy season (0.49–0.50), suggesting that the interpolation methods capture about half of the observed variability during the monsoon months.

#### 5.2.3 Spatial Patterns

The interpolated SIS surfaces reveal consistent spatial patterns across all methods:

1. **North-south gradient:** SIS increases from south to north, corresponding to the transition from cloudy coastal zones to clear-sky Saharan conditions.

2. **Seasonal variation:** The spatial pattern shifts markedly across the annual cycle, with the strongest contrasts during the transition seasons.

3. **Local variations:** The station-based interpolation captures local variations associated with topographic effects and proximity to water bodies.

### 5.3 Conclusions

Based on the findings of this study, the following conclusions are drawn:

1. **Geostatistical methods, particularly Ordinary Kriging, provide the most accurate spatial models of solar radiation in data-sparse tropical regions.** The explicit modeling of spatial correlation through the variogram enables Kriging to assign optimal interpolation weights, resulting in superior prediction accuracy. This conclusion is supported by the consistently lowest RMSE and MAE values achieved by Ordinary Kriging across all months, as well as its high R² values.

2. **The choice of interpolation method has a significant impact on the accuracy of spatial SIS estimates.** The difference between the best (Kriging, RMSE = 3.08 Wh/m²) and worst (GPI, RMSE = 313.07 Wh/m²) performing methods is two orders of magnitude, underscoring the importance of method selection. Practitioners should carefully evaluate multiple methods before selecting one for operational use.

3. **The cross-validation regression slopes** (approximately 0.49–0.88) indicate that all interpolation methods tend to underestimate the true variability of SIS, a characteristic that should be considered when using interpolated products for applications sensitive to extreme values. The smoothing effect is most pronounced during the rainy season, when slope values drop to approximately 0.49–0.50.

4. **The comparative visualization** of results through side-by-side monthly maps enables researchers, policymakers, and solar energy planners to visually assess the consistency of predictions across methods and identify spatial patterns in interpolation performance.

5. **The 40-year SIS dataset** used in this study, combined with the spatial modeling framework, provides a valuable resource for understanding long-term solar radiation patterns and trends across West Africa. The dataset covers the period from 1983 to 2022, capturing both the dimming and brightening trends observed in other regions.

### 5.4 Recommendations

Based on the findings and conclusions of this study, the following recommendations are made:

#### 5.4.1 For Research and Practice

1. **Adoption of Ordinary Kriging:** Researchers and practitioners working on solar radiation mapping in West Africa and similar tropical regions should prioritize Ordinary Kriging as the primary interpolation method, given its superior accuracy and provision of uncertainty estimates. The variogram should be carefully fitted using weighted least squares, and the exponential model is recommended as a suitable default for solar radiation data.

2. **Complementary use of RBF:** Where variogram modeling expertise is limited, RBF (particularly thin-plate spline) provides a good alternative that achieves nearly as much accuracy as Kriging without requiring variogram analysis. RBF is particularly suitable for producing smooth, continuous radiation surfaces.

3. **Use of EBK for non-stationary data:** EBK should be considered when the spatial distribution of solar radiation exhibits strong non-stationarity or non-Gaussian characteristics, as its automated variogram fitting and log-normal transformation can provide advantages in such situations. However, users should be aware that EBK may not always outperform Ordinary Kriging for well-behaved data.

4. **Avoidance of polynomial methods:** GPI and LPI should not be used as primary interpolation methods for solar radiation in regions with complex climatic gradients, though they may be useful as detrending tools in hybrid approaches such as regression-kriging.

#### 5.4.2 For Solar Energy Planning

5. **Integration with solar resource assessments:** The interpolated SIS products developed in this study should be integrated into solar energy resource assessments and feasibility studies for photovoltaic and concentrated solar power projects across West Africa. The monthly resolution of the data allows for seasonal planning of solar energy installations.

6. **Seasonal planning:** The seasonal variation in SIS accuracy should be considered when using interpolated products for solar energy planning. Higher confidence can be placed in dry-season estimates, while rainy-season estimates should be used with appropriate uncertainty margins. The cross-validation regression parameters (slope and intercept) can be used to adjust predictions for systematic bias.

7. **Decision support:** The comparative results and accuracy metrics produced in this study should be made available to energy planners and policymakers as a resource for exploring solar radiation patterns and selecting appropriate interpolation approaches for solar energy resource assessment.

8. **Capacity building:** Training programs should be developed to build local capacity in geostatistical methods for solar radiation mapping. This would enable national meteorological agencies and energy planning organizations in West Africa to produce their own spatial radiation estimates using the methods and tools demonstrated in this study.

#### 5.4.3 For Future Research

9. **Incorporation of auxiliary variables:** Future studies should explore the use of co-kriging or regression-kriging approaches that incorporate auxiliary variables such as cloud cover, aerosol optical depth, elevation, and humidity to improve prediction accuracy. These variables are available from satellite remote sensing and reanalysis datasets and could significantly improve interpolation performance, particularly in data-sparse areas.

10. **Satellite-ground data fusion:** The combination of satellite-derived SIS estimates with ground-based measurements using data fusion techniques could provide improved spatial coverage and accuracy. Li et al. (2013) demonstrated the potential of such approaches for solar radiation mapping in China, and similar methods could be applied to West Africa.

11. **Sub-daily analysis:** Extending the analysis to sub-daily time scales (hourly or 3-hourly) would provide more detailed information for solar energy applications, particularly for system sizing and dispatch optimization. Sub-daily analysis would also capture the diurnal cycle of solar radiation, which is important for matching solar energy supply with demand patterns.

12. **Machine learning comparisons:** Future work should compare geostatistical methods with machine learning approaches (such as random forests, gradient boosting, and neural networks) for solar radiation interpolation. Mubiru et al. (2008) demonstrated that neural networks can outperform traditional interpolation methods in areas with complex terrain, and recent advances in machine learning may provide further improvements.

13. **Uncertainty quantification:** More detailed analysis of prediction uncertainty, including the production of probability maps and confidence intervals, would enhance the utility of interpolated SIS products for risk-sensitive applications. The kriging variance provided by Ordinary Kriging could be used to generate uncertainty maps that complement the predicted SIS surfaces.

14. **Temporal trend analysis:** The 40-year dataset used in this study provides an opportunity to investigate long-term trends and changes in solar radiation across West Africa, which could have implications for climate change adaptation and solar energy planning. Trend analysis could also identify regions experiencing significant changes in solar radiation that may require updated planning strategies.

15. **Network optimization:** Future studies should investigate the optimal design of monitoring networks for solar radiation in West Africa, using the interpolation accuracy results from this study to identify areas where additional stations would most improve prediction accuracy. This could guide investments in new meteorological infrastructure.

### 5.5 Contribution to Knowledge

This study makes the following contributions to knowledge:

1. It provides the **first comprehensive comparison of six spatial interpolation methods for SIS across the entire West African region**, filling a gap in the literature on solar radiation modeling in data-sparse tropical areas. The study covers 108 stations across 17 countries and 4 climatic zones, providing a uniquely comprehensive evaluation.

2. It demonstrates the **applicability of Empirical Bayes Kriging for solar radiation interpolation in the tropics**, a method that has received limited attention in this context. While EBK did not outperform Ordinary Kriging in this study, its performance was comparable, suggesting that it is a viable alternative in certain situations.

3. It presents the results through **comparative spatial maps** that make the findings accessible and enable non-specialists to visually explore and compare different interpolation approaches across the study area.

4. It provides a **40-year climatological analysis of SIS across West Africa**, with detailed seasonal and spatial patterns that can serve as a baseline for future studies. The analysis reveals the dominant controls on solar radiation in the region and identifies the key challenges for spatial interpolation.

5. It provides **practical guidance for solar energy planning** in West Africa, including recommendations for interpolation methods, seasonal planning considerations, and uncertainty quantification approaches.
