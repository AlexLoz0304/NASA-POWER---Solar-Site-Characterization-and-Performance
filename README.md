# NASA-POWER---Solar-Site-Characterization-and-Performance

Alexander Lozano, Alexander P Verhaeghe

## Data Source 

https://power.larc.nasa.gov/api/pages

NASA POWER (Prediction of Worldwide Energy Resources) is a NASA project that provides free, globally available solar and meteorological data derived from satellite observations and atmospheric models. It exposes a REST API returning JSON, with daily data going back to 1981, covering any lat/lon coordinate on Earth. No authentication is required. It returns a daily time series of over 300+ parameters for any geographical latitude and longitude.

Parameters used:
ALLSKY_SFC_SW_DWN — Global Horizontal Irradiance (GHI)
CLRSKY_SFC_SW_DWN — Clear sky GHI, used to compute cloud loss fraction
ALLSKY_SFC_SW_DNI — Direct Normal Irradiance, for tilt/tracking analysis
ALLSKY_SFC_SW_DIFF — Diffuse irradiance component
ALLSKY_KT — Clearness index, primary site quality metric
PSH — Peak Sun Hours, pre-computed daily yield proxy
T2M — Temperature at 2 m, used for efficiency temperature correction
T2M_MAX — Daily max temperature, worst-case thermal derating
AOD_55 — Aerosol optical depth, quantifies haze/dust attenuation
CLOUD_AMT_DAY — Daytime cloud fraction, intermittency characterization

## Build

We plan to build a FastAPI application that lets users manage geographic locations, automatically fetches solar and climate data for each location from NASA POWER, and stores it in a Redis database. On top of that stored data, the app will have analysis endpoints that compute metrics like temperature-corrected PV yield, cloud loss fraction, and a site suitability score — allowing users to compare locations and identify the best candidates for solar panel installation. This is subject to change, just our original idea
