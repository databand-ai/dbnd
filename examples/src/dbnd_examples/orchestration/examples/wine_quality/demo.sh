#!/usr/bin/env bash

dbnd run predict_wine_quality  --data dbnd_examples/data/wine_quality_minimized.csv
dbnd run predict_wine_quality  --data dbnd_examples/data/wine_quality_minimized.csv --alpha 0.3
dbnd run predict_wine_quality_parameter_search  --predict-wine-quality-data dbnd_examples/data/wine_quality_minimized.csv


dbnd run predict_wine_quality  --task-env prod --alpha 0.2 --l1-ratio 0.4  --cloud aws
dbnd run predict_wine_quality_parameter_search  --predict-wine-quality-data dbnd_examples/data/wine_quality_minimized.csv
dbnd run predict_wine_quality  --task-env prod --alpha 0.2 --l1-ratio 0.4  --fetch-data-period 30d
