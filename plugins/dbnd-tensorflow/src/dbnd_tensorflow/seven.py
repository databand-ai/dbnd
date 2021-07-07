import tensorflow


if hasattr(tensorflow, "python"):
    from tensorflow.python.keras.callbacks import History
    from tensorflow.python.keras import models
else:
    from tensorflow.keras.callbacks import History
    from tensorflow.keras import models
