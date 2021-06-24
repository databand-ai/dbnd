import os
import pickle
import sys

from scoring_server import init


PYFUNC_NAME = "./pyfunc"
# PYFUNC_NAME = "result.pickle"


def load_pyfunc():
    # data_path = os.path.join(PYFUNC_PATH, PYFUNC_NAME)
    with open(PYFUNC_NAME, "rb") as fb:
        return pickle.load(fb)


if __name__ == "__main__":
    try:
        port = int(sys.argv[1])
    except Exception:
        exit(1)
    model = load_pyfunc()
    app = init(model)
    app.run(host="0.0.0.0", port=port, debug=True)
