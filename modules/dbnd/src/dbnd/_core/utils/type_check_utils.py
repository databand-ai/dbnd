def is_instance_by_class_name(obj, class_name):
    # If it's metaclass
    if type(obj).__class__ == type(type):
        return obj.__class__.__name__ == class_name

    for cls in obj.__class__.mro():
        if cls.__name__ == class_name:
            return True
    return False
