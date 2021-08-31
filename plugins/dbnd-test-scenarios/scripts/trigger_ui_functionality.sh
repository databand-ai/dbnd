#!/usr/bin/env bash
dbnd run dbnd_test_scenarios.pipelines.ui_functionality.all_ui_function --task-version now --set input=some_input
dbnd run dbnd_test_scenarios.pipelines.large_pipeline.large_pipe_int --task-version now --description "this is a very long description which we expect to not be fully shown"
dbnd run dbnd_test_scenarios.pipelines.bad_pipeline.bad_pipe_int --task-version now --name "bad pipeline" --description "this pipeline should fail"
dbnd run predict_wine_quality --scheduled-job-name "schedule name"
dbnd run predict_wine_quality --scheduled-job-name "schedule name" --name "should be scheduled 1"
dbnd run predict_wine_quality --scheduled-job-name "schedule name" --name "should be scheduled 2"
