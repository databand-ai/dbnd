from dbnd import log_metric


class TestDocTrackingQuickstart:
    def test_step_one(self):
        #### DOC START
        # Python 3.6.8
        from typing import Tuple

        from pandas import DataFrame, Series
        from sklearn import datasets
        from sklearn.model_selection import train_test_split

        def prepare_data() -> Tuple[DataFrame, DataFrame]:
            """load dataset from sklearn. split into training and testing sets"""
            raw_data = datasets.load_diabetes()

            # create a pandas DataFrame from sklearn dataset
            df = DataFrame(raw_data["data"], columns=raw_data["feature_names"])
            df["target"] = Series(raw_data["target"])

            # split the data into training and testing sets
            training_data, testing_data = train_test_split(df, test_size=0.25)

            return training_data, testing_data

        #### DOC END
        prepare_data()

    def test_integrating_dataframe_tracking(self):
        #### DOC START
        # Python 3.6.8
        import logging

        from typing import Tuple

        from pandas import DataFrame, Series
        from sklearn import datasets
        from sklearn.model_selection import train_test_split

        from dbnd import log_dataframe, log_metric

        logging.basicConfig(level=logging.INFO)

        def prepare_data() -> Tuple[DataFrame, DataFrame]:
            """load dataset from sklearn. split into training and testing sets"""
            raw_data = datasets.load_diabetes()

            # create a pandas DataFrame from sklearn dataset
            df = DataFrame(raw_data["data"], columns=raw_data["feature_names"])
            df["target"] = Series(raw_data["target"])

            # split the data into training and testing sets
            training_data, testing_data = train_test_split(df, test_size=0.25)

            # use DBND logging features to log DataFrames with histograms
            log_dataframe(
                "training data",
                training_data,
                with_histograms=True,
                with_schema=True,
                with_stats=True,
            )
            log_dataframe("testing_data", testing_data)

            # use DBND logging features to log the mean of s1
            log_metric("mean s1", training_data["s1"].mean())

            return training_data, testing_data

        #### DOC END
        prepare_data()

    def test_step_two(self):
        #### DOC START
        # Python 3.6.8
        import logging

        from typing import Tuple

        from pandas import DataFrame, Series
        from sklearn import datasets
        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import mean_squared_error
        from sklearn.model_selection import train_test_split

        from dbnd import log_dataframe

        logging.basicConfig(level=logging.INFO)

        def prepare_data() -> Tuple[DataFrame, DataFrame]:
            """load dataset from sklearn. split into training and testing sets"""
            raw_data = datasets.load_diabetes()

            # create a pandas DataFrame from sklearn dataset
            df = DataFrame(raw_data["data"], columns=raw_data["feature_names"])
            df["target"] = Series(raw_data["target"])

            # split the data into training and testing sets
            training_data, testing_data = train_test_split(df, test_size=0.25)

            # use DBND logging features to log DataFrames with histograms
            log_dataframe(
                "training data",
                training_data,
                with_histograms=True,
                with_schema=True,
                with_stats=True,
            )
            log_dataframe("testing_data", testing_data)

            # use DBND logging features to log the mean of s1
            log_metric("mean s1", training_data["s1"].mean())

            return training_data, testing_data

        def train_model(training_data: DataFrame) -> LinearRegression:
            """train a linear regression model"""
            model = LinearRegression()

            # train a linear regression model
            model.fit(training_data.drop("target", axis=1), training_data["target"])
            return model

        def test_model(model: LinearRegression, testing_data: DataFrame) -> str:
            """test the model, output mean squared error and r2 score"""
            testing_x = testing_data.drop("target", axis=1)
            testing_y = testing_data["target"]
            predictions = model.predict(testing_x)

            mse = mean_squared_error(testing_y, predictions)
            r2_score = model.score(testing_x, testing_y)

            return f"MSE: {mse}, R2: {r2_score}"

        #### DOC END
        test_model(
            model=train_model(training_data=prepare_data()[0]),
            testing_data=prepare_data()[1],
        )

    def test_tracking_more_metrics(self):
        #### DOC START
        import logging

        from typing import Tuple

        from pandas import DataFrame, Series
        from sklearn import datasets
        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import mean_squared_error
        from sklearn.model_selection import train_test_split

        from dbnd import log_dataframe, log_metric

        logging.basicConfig(level=logging.INFO)

        def prepare_data() -> Tuple[DataFrame, DataFrame]:
            """load dataset from sklearn. split into training and testing sets"""
            raw_data = datasets.load_diabetes()

            # create a pandas DataFrame from sklearn dataset
            df = DataFrame(raw_data["data"], columns=raw_data["feature_names"])
            df["target"] = Series(raw_data["target"])

            # split the data into training and testing sets
            training_data, testing_data = train_test_split(df, test_size=0.25)

            # use DBND logging features to log DataFrames with histograms
            log_dataframe(
                "training data",
                training_data,
                with_histograms=True,
                with_schema=True,
                with_stats=True,
            )
            log_dataframe("testing_data", testing_data)

            # use DBND logging features to log the mean of s1
            log_metric("mean s1", training_data["s1"].mean())

            return training_data, testing_data

        def train_model(training_data: DataFrame) -> LinearRegression:
            """train a linear regression model"""
            model = LinearRegression()

            # train a linear regression model
            model.fit(training_data.drop("target", axis=1), training_data["target"])

            # use DBND log crucial details about the regression model with log_metric:
            log_metric("model intercept", model.intercept_)  # logging a numeric
            log_metric("coefficients", model.coef_)  # logging an np array
            return model

        def test_model(model: LinearRegression, testing_data: DataFrame) -> str:
            """test the model, output mean squared error and r2 score"""
            testing_x = testing_data.drop("target", axis=1)
            testing_y = testing_data["target"]
            predictions = model.predict(testing_x)
            mse = mean_squared_error(testing_y, predictions)
            r2_score = model.score(testing_x, testing_y)

            # use DBND log_metric to capture important model details:
            log_metric("mean squared error:", mse)
            log_metric("r2 score", r2_score)

            return f"MSE: {mse}, R2: {r2_score}"

        # if __name__ == '__main__':
        training_set, testing_set = prepare_data()
        model = train_model(training_set)
        metrics = test_model(model, testing_set)
        #### DOC END

        assert metrics
