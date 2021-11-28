Changes that were made for databand compatibility purposes:
pendulum.py:
    Line 71, 101 - Changed the if statements to ensure that when un-vendorized pendulum is used we don't have isinstance clashes (Same class, different fully qualified name, one from _vendor, one from the official pendulum site-packages)
