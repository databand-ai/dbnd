# © Copyright Databand.ai, an IBM Company 2022

import logging

import matplotlib
import numpy as np

from matplotlib.figure import Figure

from dbnd import task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase


logger = logging.getLogger(__name__)

matplotlib.use("Agg")


@task
def t_f_matplotlib_return():
    # type: ()-> Figure
    import matplotlib.pyplot as plt

    fig = plt.figure()
    ax1 = fig.add_subplot(2, 2, 1)
    ax1.hist(np.random.randn(100), bins=20, alpha=0.3)
    ax2 = fig.add_subplot(2, 2, 2)
    ax2.scatter(np.arange(30), np.arange(30) + 3 * np.random.randn(30))
    fig.add_subplot(2, 2, 3)
    return fig


class TestMatplotlibFigureOutputs(TargetTestBase):
    def test_matplotlib_return(self):
        t_f_matplotlib_return.dbnd_run()
