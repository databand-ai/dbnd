# It differs from dbnd._vendor in a way _vendor_package package is used:
# instead of replacing all imports to dbnd._vendor, we add path to _vendor_package
# module to `sys.path`. This approach might casue versions
# conflicts that we agreed to solve one by one.
#
