# CHAPTER ONE

## INTRODUCTION

### 1.1 Background of the Study

Solar radiation is one of the most fundamental energy inputs to the Earth's surface, driving a wide range of physical, biological, and environmental processes. It plays a critical role in weather and climate systems, photosynthesis, the hydrological cycle, and the global energy balance. The accurate measurement and estimation of solar radiation is therefore essential for numerous applications, including solar energy system design, agricultural planning, hydrological modeling, and climate change studies (Li et al., 2017).

Surface Incoming Shortwave radiation (SIS), also referred to as global horizontal irradiance (GHI), represents the total solar radiation received on a horizontal surface at the Earth's ground level. It is composed of both direct normal irradiance (DNI), which is the solar radiation received directly from the solar disk, and diffuse horizontal irradiance (DHI), which is the solar radiation received from the sky after being scattered by the atmosphere (Duffie and Beckman, 2013). The magnitude of SIS at any given location is influenced by a complex interplay of factors, including latitude, altitude, cloud cover, aerosol concentration, atmospheric water vapor, and surface albedo (Wild et al., 2015).

The spatial distribution of solar radiation across a given region is not uniform. It varies systematically with latitude (due to the solar zenith angle), with altitude (due to atmospheric path length), and with local conditions such as cloudiness, pollution, and terrain. These spatial variations have important implications for applications that require accurate, location-specific solar radiation estimates. For example, the design of a photovoltaic power plant requires knowledge of the expected solar radiation at the specific site, while agricultural planning requires information about the spatial distribution of solar radiation across a farming region (Kasten and Czeplak, 1980).

In West Africa, solar radiation patterns are particularly variable due to the region's diverse climatic zones, ranging from the hyper-arid Saharan edge in the north to the humid tropical coast in the south. The Sahel belt, which traverses the central portion of West Africa, experiences significant seasonal and inter-annual variability in solar radiation, driven by the movement of the Intertropical Convergence Zone (ITCZ) and the resulting monsoon dynamics (Nikulin et al., 2012). Understanding the spatial distribution of solar radiation across this region is crucial for solar energy planning, which has been identified as a key pathway for addressing the chronic energy deficit affecting many West African nations.

Despite the growing importance of solar radiation data, ground-based measurements remain sparse across much of West Africa. The network of meteorological stations is unevenly distributed, with many areas—particularly rural and remote regions—lacking adequate observational coverage. This spatial sparsity of measurement points presents a significant challenge for obtaining continuous, spatially complete radiation fields needed for regional assessments and applications (Perez et al., 2013). The International Renewable Energy Agency (IRENA, 2022) has estimated that West Africa receives some of the highest solar radiation levels in the world, with annual average SIS values exceeding 5.0 kWh/m²/day across most of the region. However, the lack of comprehensive, spatially resolved radiation data limits the ability of planners and policymakers to fully capitalize on this resource.

The challenge of spatial data scarcity can be addressed through spatial interpolation techniques, which estimate values at unsampled locations based on observations at nearby measurement points. Among the various interpolation methods available, geostatistical techniques such as Kriging offer distinct advantages because they explicitly account for the spatial correlation structure of the data and provide measures of prediction uncertainty (Webster and Oliver, 2007). The development of Empirical Bayes Kriging (EBK) has further extended the capabilities of geostatistical methods by providing automated variogram fitting and handling of non-Gaussian data distributions (Krivoruchko, 2012).

### 1.2 Problem Statement

The accurate spatial estimation of solar radiation across a region requires interpolation from discrete point measurements to a continuous surface. Traditional interpolation methods, such as simple arithmetic averaging or Thiessen polygons, do not account for the spatial correlation structure inherent in meteorological data and often produce unreliable estimates, particularly in areas distant from measurement stations.

Geostatistical methods offer a more rigorous framework for spatial interpolation by explicitly modeling the spatial dependence structure of the data through the variogram function. Techniques such as Kriging, Empirical Bayes Kriging (EBK), and their variants provide not only predictions at unsampled locations but also measures of prediction uncertainty, which are invaluable for decision-making (Webster and Oliver, 2007). However, the selection of the most appropriate geostatistical method for a given application and region is not straightforward.

Different methods make different assumptions about the stationarity of the mean and variance, the form of the spatial correlation function, and the noise characteristics of the data. The performance of these methods can vary significantly depending on the density and configuration of the sampling network, the nature of the spatial variability, and the specific characteristics of the variable being estimated (Chiles and Delfiner, 2012). For solar radiation in West Africa, several unique challenges arise:

1. **Sparse and unevenly distributed station networks:** The density of meteorological stations varies by more than an order of magnitude across the region, from densely instrumented areas in southern Nigeria to virtually unmeasured regions in northern Chad and Mauritania.

2. **Diverse climatic zones:** The study area spans four distinct climatic zones, each with different patterns of solar radiation variability, from the consistent clear skies of the Sahara to the highly variable cloud cover of the humid coast.

3. **Seasonal dynamics:** The monsoon cycle creates dramatic seasonal shifts in solar radiation patterns, with the spatial distribution of SIS changing fundamentally between the dry and wet seasons.

4. **Lack of comprehensive comparative studies:** While individual studies have applied specific methods to specific regions, there is a lack of systematic comparative evaluations of multiple geostatistical methods for solar radiation across the full diversity of West African climates.

There is therefore a need for a systematic comparative evaluation of geostatistical interpolation methods for solar radiation in the West African context, using cross-validation techniques to assess prediction accuracy and identify the most suitable approach for operational applications.

### 1.3 Aim and Objectives

**Aim:**

The aim of this study is to develop spatial models for the accurate prediction of solar radiation across West Africa using geostatistical analysis techniques, and to evaluate the performance of multiple interpolation methods through cross-validation.

**Objectives:**

The specific objectives of this study are:

1. To collect, preprocess, and analyze surface incoming shortwave radiation (SIS) data from 108 meteorological stations across West Africa over the period 1983–2022, representing a 40-year climatological record.

2. To apply and compare six spatial interpolation methods—Inverse Distance Weighting (IDW), Ordinary Kriging, Empirical Bayes Kriging (EBK), Radial Basis Function (RBF), Local Polynomial Interpolation (LPI), and Global Polynomial Interpolation (GPI)—for estimating solar radiation across the study area.

3. To implement a cross-validation framework using ArcGIS Geostatistical Analyst to evaluate the prediction accuracy of each method, using the root mean square error (RMSE), mean absolute error (MAE), mean error (ME), median absolute error (MedAE), and regression slope and intercept as performance metrics.

4. To identify the most suitable geostatistical method for spatial modeling of solar radiation in West Africa based on the cross-validation results, and to provide recommendations for practitioners and researchers.

5. To present the spatial outputs and accuracy metrics of the different methods through comparative maps and statistical tables.

### 1.4 Significance of the Study

This study contributes to the growing body of knowledge on spatial modeling of solar radiation in data-sparse regions. The findings are expected to be beneficial in several ways:

1. **Solar Energy Planning:** The spatial models developed in this study can be used to identify optimal locations for solar energy installations across West Africa, supporting the region's transition to renewable energy sources. Accurate SIS maps are essential for solar resource assessments, which inform decisions about panel orientation, system sizing, and expected energy yield.

2. **Agricultural Applications:** Accurate solar radiation maps are essential for crop modeling, evapotranspiration estimation, and irrigation planning, all of which are critical for food security in the region. Solar radiation is a key input to crop growth models, and spatial interpolation of SIS can improve the accuracy of agricultural productivity estimates across data-sparse areas.

3. **Climate Studies:** The long-term spatial radiation fields produced by this study can serve as inputs for regional climate models and for monitoring changes in solar radiation over time. The 40-year record analyzed in this study provides a baseline against which future changes in solar radiation can be assessed.

4. **Methodological Contribution:** The comparative evaluation of multiple geostatistical methods provides guidance for practitioners on the selection of appropriate interpolation techniques for solar radiation in similar climatic settings. The identification of Ordinary Kriging as the most accurate method, and the evaluation of the newer EBK method, contribute to the methodological literature on spatial interpolation.

5. **Open Science:** The development of an interactive visualization tool makes the results accessible to a broader audience, including policymakers, energy planners, and the research community. The tool enables non-specialists to explore and compare different interpolation approaches without requiring specialized software.

### 1.5 Scope and Limitations

**Scope:**

This study focuses on the spatial modeling of Surface Incoming Shortwave radiation (SIS) across West Africa, covering the geographical extent approximately between latitudes 4°N and 27°N and from longitude 18°W to 16°E. The study area encompasses the Sahel, Sudan, Guinea savanna, and coastal zones of West Africa, including countries such as Nigeria, Ghana, Senegal, Mali, Burkina Faso, Niger, and others. The temporal coverage spans from 1983 to 2022, with monthly mean SIS values analyzed.

The study compares six spatial interpolation methods: IDW, Ordinary Kriging, EBK, RBF, LPI, and GPI. Cross-validation is performed using ArcGIS Geostatistical Analyst, and the results are presented through monthly spatial maps and comparative statistical tables.

**Limitations:**

1. The study is limited to the SIS variable and does not extend to other solar radiation components such as direct normal irradiance or photosynthetically active radiation.

2. The spatial resolution of the analysis is constrained by the density of the meteorological station network, which varies significantly across the study area. In areas with very sparse station coverage (such as northern Mauritania and Chad), the interpolation uncertainty is likely to be high regardless of the method used.

3. The cross-validation is performed on a monthly climatological basis and does not account for sub-monthly variability or extreme events.

4. The geostatistical methods are implemented using the ArcGIS Geostatistical Analyst framework, and results may differ if alternative software or implementations are used.

5. The study does not incorporate auxiliary variables (such as elevation, cloud cover, or aerosol optical depth) that could potentially improve prediction accuracy in co-kriging or regression-kriging approaches.

6. The analysis uses satellite-derived and ground-calibrated SIS data rather than purely ground-based measurements, which introduces additional uncertainty related to the satellite retrieval algorithms.

### 1.6 Organization of the Study

This thesis is organized into five chapters:

- **Chapter One** introduces the study, providing the background, problem statement, aim and objectives, significance, and scope.

- **Chapter Two** presents a review of the relevant literature on solar radiation measurement and estimation, geostatistical methods for spatial interpolation, and previous studies on solar radiation modeling in Africa and globally. The chapter establishes the theoretical foundations for the methods used in this study and identifies the research gaps that this study addresses.

- **Chapter Three** describes the study area, data sources, and the methodology employed, including data preprocessing, the six interpolation methods, and cross-validation procedures. Detailed mathematical formulations are provided for each method.

- **Chapter Four** presents the results of the spatial interpolation analysis, including maps, comparison tables, and accuracy metrics for each method, along with a comprehensive discussion of the findings in the context of previous studies.

- **Chapter Five** provides a summary of the key findings, conclusions drawn from the analysis, and recommendations for future work and practical applications.
