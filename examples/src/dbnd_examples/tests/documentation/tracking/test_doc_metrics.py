# © Copyright Databand.ai, an IBM Company 2022

# rewrote


class TestDocMetrics:
    def test_doc(self):
        #### DOC START
        from dbnd import log_metric

        def calculate_alpha(alpha, beta):
            sum = alpha + beta
            log_metric("sum", sum)

            return alpha

        calculate_alpha(0.5, 0.03)
        #### DOC END
