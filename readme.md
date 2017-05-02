# Taxonomic Information Registry

This repository contains code for an experimental component in the Biogeographic Information System we are building. This system basically handles the processing of discovered taxa of interest across our various systems, does some work to align them with taxonomic authorities, and retrieves other properties from relevant data systems into a cache for use throughout the BIS. We are trying to build this as an entirely code-driven system with occasional need for human intervention to tweak config parameters to make the processing better.

The codes here all interact with a still-experimental data platform we are working with as a component of the BIS. You'll see references to API calls that you can dig around in, but a separate config file is needed to set up API keys and other parameters for use. That file has to go along with the codes when running on a processing engine somewhere.

I started all of these with Jupyter Notebooks. If you see the same names as .py files, those were written with nbconvert to run outside the Jupyter environment. I'm working to make sure everything here will port over to the experimental microservices architecture we are developing where we will run this stuff live.

## Provisional Software Disclaimer
Under USGS Software Release Policy, the software codes here are considered preliminary, not released officially, and posted to this repo for informal sharing among colleagues.

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.