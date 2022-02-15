class TestDocExplicitDatasetLogging:
    def test_doc(self):
        """
        #### DOC START
        from dbnd import dataset_op_logger

        # Read dataset example
        with dataset_op_logger(path, "read") as logger:
            df = read(path, ...)
            logger.set(data=df)

        # Write dataset example
        df = DataFrame(data)
        with dataset_op_logger(path, "write") as logger:
            write(df, ...)
            logger.set(data=df)


        with dataset_op_logger("location://path/to/value.csv", "read"):
            value = read_from()
            logger.set(data=value)
            # Read is successful

        calculate_alpha()


        with dataset_op_logger("location://path/to/value.csv", "read") as logger:
            value = read_from()
            logger.set(data=value)
            # Read is successful
            calculate_alpha()
            # If calculate_alpha raises an exception, a failed read operation is reported to Databand.


        @task()
        def prepare_data():
            log_dataset_op(
                "/path/to/value.csv",
                DbndDatasetOperationType.read,
                data=pandas_data_frame,
                with_preview=True,
                with_schema=True,
            )

        import databricks.koalas as ks
        from targets.values import DataFrameValueType, register_value_type

        class KoalasValueType(DataFrameValueType):
            type = ks.DataFrame
            type_str = "KoalasDataFrame"

        register_value_type(KoalasValueType())
        #### DOC END
        prepare_data.dbnd_run()
        """
