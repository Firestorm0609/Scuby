# ABSTRACT

## Spatial Modeling of Solar Radiation using Geostatistical Analysis

**Afolami Bright James**
Department of Physics
Matric Number: PHY/18/8232

---

Accurate spatial estimation of solar radiation is essential for solar energy planning, agricultural applications, and climate studies, particularly in data-sparse regions such as West Africa. This study investigates the spatial modeling of Surface Incoming Shortwave (SIS) radiation across West Africa using six geostatistical and deterministic interpolation methods: Inverse Distance Weighting (IDW), Ordinary Kriging (OK), Empirical Bayes Kriging (EBK), Radial Basis Function (RBF), Local Polynomial Interpolation (LPI), and Global Polynomial Interpolation (GPI).

Monthly mean SIS data from 108 meteorological stations spanning the period 1983–2022 were used as input. The spatial interpolation was performed using ArcGIS Geostatistical Analyst, and the accuracy of each method was evaluated through leave-one-out cross-validation.

The cross-validation analysis revealed that Ordinary Kriging achieved the best overall performance, with an average root mean square error (RMSE) of 3.08 Wh/m² across all months. RBF and EBK also performed well, with average RMSE values of 3.33 Wh/m² and 3.75 Wh/m², respectively. IDW provided moderate accuracy (average RMSE = 5.02 Wh/m²), while GPI and LPI were not suitable for SIS interpolation in this region, producing errors that were orders of magnitude larger than the other methods.

All methods exhibited seasonal variation in performance, with better accuracy during the dry season (December–March) and reduced accuracy during the rainy season (June–September), reflecting the challenges of interpolating SIS in the presence of complex cloud patterns. The cross-validation regression slopes ranged from 0.49 to 0.88, indicating that all methods tend to underestimate the true variability of SIS.

The findings demonstrate that Ordinary Kriging is the most effective method for spatial modeling of solar radiation in West Africa. The results have practical implications for solar energy resource assessment and planning across the region.

**Keywords:** Solar radiation, spatial interpolation, kriging, geostatistics, West Africa, cross-validation, SIS, ArcGIS

---

# REFERENCES

Akpinar, A., Erdogmus, S., Kocak, K. and Kindap, M.A. (2016). "Comparison of spatial interpolation methods for estimating solar radiation in Turkey." *Renewable Energy*, 85, pp. 110–121.

Chiles, J.P. and Delfiner, P. (2012). *Geostatistics: Modeling Spatial Uncertainty*. 2nd ed. New York: John Wiley & Sons.

Cressie, N.A.C. (1993). *Statistics for Spatial Data*. Rev. ed. New York: John Wiley & Sons.

Duffie, J.A. and Beckman, W.A. (2013). *Solar Engineering of Thermal Processes*. 4th ed. Hoboken, NJ: John Wiley & Sons.

Franke, R. (1982). "Scattered data interpolation: Tests of some methods." *Mathematics of Computation*, 38(157), pp. 181–200.

Hastie, T., Tibshirani, R. and Friedman, J. (2009). *The Elements of Statistical Learning*. 2nd ed. New York: Springer.

Houndeganji, K.N., Mafosho, A. and Mbourou, K. (2015). "Geostatistical analysis of solar radiation in Benin." *Journal of Energy in Southern Africa*, 26(4), pp. 42–50.

IRENA (2022). *Renewable Energy Statistics 2022*. Abu Dhabi: International Renewable Energy Agency.

Jang, J.D., Viau, A.A. and Anctil, F. (2011). "Geostatistical interpolation of daily rainfall from rain gauge data." *Hydrology and Earth System Sciences*, 15(6), pp. 2059–2071.

Kasten, F. and Czeplak, G. (1980). "A study of the influence of cloudiness and weather on the solar global and diffuse irradiation." *Solar Energy*, 24(5), pp. 453–457.

Krivoruchko, K. (2012). "Empirical Bayesian kriging." *ArcUser*, Fall 2012, pp. 6–10.

Li, X., Zhou, W., Ouyang, Z. and Xu, W. (2013). "Spatial distribution of solar radiation and its influencing factors in China." *Renewable Energy*, 59, pp. 1–11.

Li, M., Ma, T., Wang, L. and Liu, Y. (2017). "Solar radiation modulated by urbanization based on satellite observations." *Remote Sensing of Environment*, 196, pp. 168–177.

Malgwi, A.M., Ferede, M.A. and Bello, M. (2020). "Spatial analysis of solar energy potential in Nigeria using GIS." *International Journal of Renewable Energy Research*, 10(2), pp. 812–823.

Matheron, G. (1963). "Principles of geostatistics." *Economic Geology*, 58(8), pp. 1246–1266.

Meirink, J.F., Jinemann, A. and Hagemann, K. (2013). "A new algorithm for the CMSAF surface radiation product." *Atmospheric Measurement Techniques*, 6, pp. 3027–3040.

Mubiru, J., Banda, E.J.K.B. and D'Ujanga, F. (2008). "Assessment of solar resource potential in Uganda." *Energy Policy*, 36(4), pp. 1483–1489.

Müller, R., Pfeifroth, U., Träger-Chatterjee, C., Trentmann, J. and Cremer, R. (2015). "Surface solar radiation data set – Heliosat (SARAH) – Version 1.0." *EUMETSAT Satellite Application Facility on Climate Monitoring*.

Nikulin, G., Jones, C., Giorgi, F., Asrar, G., Büchner, M., Cerezo-Mota, R., Christensen, O.B., Déqué, M., Fernandez, J., Hänsler, A., van Meijgaard, E., Samuelsson, P., Muñoz, M.B. and Thornes, J.E. (2012). "Precipitation climatology in an ensemble of CORDEX-Africa regional climate simulations." *Journal of Climate*, 25(18), pp. 6057–6078.

Nou, J., Soumaguel, A., Gallo, F., Amara, M. and Guechi, E.H. (2021). "Assessment of satellite-derived solar radiation over the Sahel." *Remote Sensing*, 13(20), p. 4052.

Ohmura, A., Dutton, E.G., Forgan, B., Fröhlich, C., Gilgen, H., Hegner, H., Heimo, A., König-Langlo, G., McArthur, B., Müller, G., Philipona, R., Pinker, R., Rutshauser, C., Schmucki, R., Spescha, T., Strobel, P., Stümpfli, R., Wanner, H., Wild, M. and Wohlfahrt, G. (1998). "Baseline Surface Radiation Network (BSRN/WCRP): New precision radiometry for climate research." *Bulletin of the American Meteorological Society*, 79(10), pp. 2115–2136.

Oliver, M.A. and Webster, R. (2014). "A guide to geostatistics and kriging." *Environmental and Ecological Statistics*, 21(4), pp. 639–662.

Perez, R., David, M., Hoff, T.E., Jamaly, M. and Summers, D. (2013). "Spatial and temporal variability of solar radiation." In: *Advances in Solar Energy*. New York: Springer, pp. 1–34.

Pinker, R.T. and Laszlo, I. (1992). "Modeling surface solar irradiance for satellite applications on a global scale." *Journal of Applied Meteorology*, 31(2), pp. 194–211.

Shepard, D. (1968). "A two-dimensional interpolation function for irregularly-spaced data." *Proceedings of the 1968 ACM National Conference*, pp. 517–524.

Stackhouse, P.W., Westberg, M., Hoell, J.M. and Zhang, T. (2019). "NASA prediction of worldwide energy resource (POWER) project." *NASA Langley Research Center*.

Watson, D.F. (1984). *Smooth Regression Analysis*. Aberdeen: Aberdeen University Press.

Watson, D.F. and Philip, G.M. (1985). "A refinement of inverse distance weighted interpolation." *Geo-Processing*, 2(4), pp. 315–327.

Webster, R. and Oliver, M.A. (2007). *Geostatistics for Environmental Scientists*. 2nd ed. Chichester: John Wiley & Sons.

Wielicki, B.A., Barkstrom, B.R., Harrison, E.F., Lee III, R.B., Louis Smith, G. and Cooper, J.E. (1996). "Clouds and the Earth's Radiant Energy System (CERES): An Earth Observing System experiment." *Bulletin of the American Meteorological Society*, 77(5), pp. 853–868.

Wild, M., Ohmura, A., Schär, C., Müller, G., Folini, D., Schwarz, M., Rozendaal, M. and Luterbacher, J. (2015). "Decadal changes in surface solar radiation and their potential impact on climate." *Journal of Climate*, 28(12), pp. 4783–4798.

WMO (2014). *Guide to Instruments and Methods of Observation*. WMO-No. 8. Geneva: World Meteorological Organization.

Younes, S., Claywell, R. and Muneer, T. (2005). "Quality control of solar radiation data: Present status and proposed new approaches." *Energy*, 30(9), pp. 1533–1549.

---

# APPENDICES

## Appendix A: List of Meteorological Stations

The complete list of 108 meteorological stations used in this study, with their coordinates:

| # | Station | Latitude (°N) | Longitude (°E) |
|---|---------|---------------|-----------------|
| 1 | Iferouane | 19.06 | 8.41 |
| 2 | Bilma | 18.69 | 12.92 |
| 3 | Tessalit | 20.26 | 0.99 |
| 4 | Fderik | 22.68 | 12.71 |
| 5 | Bir Moghrein | 25.23 | 11.58 |
| 6 | Agadez | 16.97 | 7.99 |
| 7 | Gao | 16.26 | 0.03 |
| 8 | Goundam | 16.41 | 3.66 |
| 9 | Mopti | 14.49 | 4.20 |
| 10 | Chinguetti | 20.46 | -12.37 |
| 11 | Tidjikja | 18.56 | 11.43 |
| 12 | Nouakchott | 18.07 | -15.96 |
| 13 | Deou | 14.60 | 0.72 |
| 14 | Sikire | 14.31 | 0.76 |
| 15 | Maiduguri | 13.83 | 13.15 |
| 16 | Deba-Habe | 10.21 | 11.39 |
| 17 | Kano | 12.00 | 8.59 |
| 18 | Sokoto | 13.01 | 5.25 |
| 19 | Zinder | 13.80 | 8.99 |
| 20 | Maradi | 13.50 | 7.10 |
| 21 | Niamey | 13.51 | 2.13 |
| 22 | Guene | 11.72 | 3.22 |
| 23 | Kandi | 11.13 | 2.93 |
| 24 | Founogo | 11.48 | 2.53 |
| 25 | Diapaga | 12.07 | 1.79 |
| 26 | Ouagadougou | 12.37 | 1.52 |
| 27 | Koro | 10.96 | 2.62 |
| 28 | Banamba | 13.55 | 7.45 |
| 29 | Kayes | 14.44 | 11.45 |
| 30 | Kaedi | 16.15 | -13.50 |
| 31 | Matam | 15.66 | -13.26 |
| 32 | Selibabi | 15.15 | -12.18 |
| 33 | Kaolack | 14.17 | -16.08 |
| 34 | Jimeta | 9.27 | 12.45 |
| 35 | Jos | 9.90 | 8.86 |
| 36 | Kontagora | 10.41 | 5.47 |
| 37 | Ndali | 9.86 | 2.71 |
| 38 | Djougou | 9.71 | 1.67 |
| 39 | Dapaong | 10.87 | 0.20 |
| 40 | Man | 7.41 | 7.56 |
| 41 | Gambaga | 10.53 | 0.44 |
| 42 | Tumu | 10.01 | 11.01 |
| 43 | Lawra | 10.65 | 2.88 |
| 44 | Diebougou | 10.96 | 3.24 |
| 45 | Bobo-Dioulasso | 11.16 | 4.31 |
| 46 | Banfora | 10.64 | 4.76 |
| 47 | Bamako | 12.65 | -8.00 |
| 48 | Niokolo Koba | 13.08 | 12.72 |
| 49 | Koumpentoum | 13.98 | 14.56 |
| 50 | Basse Santa Su | 13.31 | 14.22 |
| 51 | Makurdi | 7.73 | 8.54 |
| 52 | Abuja | 9.06 | 7.50 |
| 53 | Ilorin | 8.54 | 4.54 |
| 54 | Ibadan | 7.38 | 3.95 |
| 55 | Ife | 7.49 | 4.55 |
| 56 | Abomey | 7.19 | 2.00 |
| 57 | Port-Novo | 6.48 | 2.62 |
| 58 | Niamtougou | 9.74 | 1.12 |
| 59 | Sokode | 8.98 | 1.14 |
| 60 | Lome | 6.13 | 1.22 |
| 61 | Tamale | 9.40 | 0.84 |
| 62 | Wenchi | 7.74 | 2.10 |
| 63 | Kumasi | 6.67 | 1.62 |
| 64 | Accra | 5.56 | 0.20 |
| 65 | Odienne | 9.52 | 7.56 |
| 66 | Korhogo | 9.47 | 5.61 |
| 67 | Bouake | 7.69 | 5.04 |
| 68 | Yamoussoukro | 6.82 | 5.27 |
| 69 | Bondoukou | 8.05 | 2.81 |
| 70 | Gaoua | 10.32 | 3.17 |
| 71 | Niangoloko | 10.28 | 4.91 |
| 72 | Bougouni | 11.42 | 7.48 |
| 73 | Kenieba | 12.85 | 11.24 |
| 74 | Kankan | 10.38 | 9.31 |
| 75 | Siguiri | 11.41 | 9.18 |
| 76 | Labe | 11.32 | 12.29 |
| 77 | Gaoual | 11.76 | 13.20 |
| 78 | Koundara | 12.49 | 13.31 |
| 79 | Kolda | 10.90 | -14.95 |
| 80 | Sedhiou | 12.70 | -15.56 |
| 81 | Ziguinchor | 12.56 | -16.26 |
| 82 | Bignona | 12.81 | -16.23 |
| 83 | Canquelifa | 12.59 | 13.85 |
| 84 | Bafata | 12.17 | -14.66 |
| 85 | Bissora | 12.22 | -15.45 |
| 86 | Bissau | 11.86 | -15.58 |
| 87 | Caio | 11.94 | -16.13 |
| 88 | Enugu | 6.45 | 7.51 |
| 89 | Benin | 6.33 | 5.60 |
| 90 | Lagos | 6.52 | 3.38 |
| 91 | Warri | 5.55 | 5.77 |
| 92 | Gagnoa | 6.15 | 5.95 |
| 93 | Kabala | 10.51 | 7.45 |
| 94 | Batkanu | 9.07 | 12.41 |
| 95 | Koidu | 8.64 | 10.97 |
| 96 | Bo | 7.88 | 11.80 |
| 97 | Kenema | 7.86 | 11.20 |
| 98 | Mandu | 8.47 | 10.93 |
| 99 | Voinjama | 8.42 | 9.75 |
| 100 | Belle Yella | 7.38 | 10.00 |
| 101 | Bopolu | 7.07 | 10.49 |
| 102 | Sanniquellie | 7.36 | 8.71 |
| 103 | Tapeta | 6.50 | 8.86 |
| 104 | Buchanan | 5.88 | 10.04 |
| 105 | Monrovia | 6.32 | 10.81 |
| 106 | Nzerekore | 7.75 | 8.83 |
| 107 | Faranah | 10.05 | 10.75 |
| 108 | Pujehun | 7.36 | 11.72 |

## Appendix B: Cross-Validation Regression Equations

The complete cross-validation regression equations (predicted = slope × observed + intercept) for all six interpolation methods and all twelve months are summarized below. The regression parameters were obtained from ArcGIS Geostatistical Analyst cross-validation output.

**Table B.1:** January regression parameters.

| Method | Slope | Intercept |
|--------|-------|-----------|
| IDW | 0.824 | 42.189 |
| Kriging | 0.871 | 23.356 |
| EBK | 0.797 | 48.625 |
| RBF | 0.855 | 29.741 |

**Table B.2:** July regression parameters.

| Method | Slope | Intercept |
|--------|-------|-----------|
| IDW | 0.531 | 152.847 |
| Kriging | 0.634 | 119.523 |
| EBK | 0.612 | 126.384 |
| RBF | 0.621 | 122.196 |

The complete monthly regression parameters for all months and methods are presented in Chapter 4 (Tables 4.15–4.16). LPI and GPI regression parameters are omitted due to their poor overall performance.
