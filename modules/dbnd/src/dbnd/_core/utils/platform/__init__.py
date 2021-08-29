import os


windows_compatible_mode = os.name == "nt"

if windows_compatible_mode:
    import colorama

    colorama.init()
