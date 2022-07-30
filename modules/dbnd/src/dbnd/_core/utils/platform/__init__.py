# © Copyright Databand.ai, an IBM Company 2022

import os


windows_compatible_mode = os.name == "nt"

if windows_compatible_mode:
    import colorama

    colorama.init()
