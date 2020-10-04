def vcolor(data, pattern):
    """ Color a graph line by line

    :param data: the data
    :type data: list of tuples (info, value)
    :param pattern: list of colors, this list defines
      the pattern to color each line of the graph.
    :type pattern: list of 'colors' (str)
    :return: the colored graph
    :rtype: list of arrays (<info>, <value>, <color>)
    """
    ret = []
    l = len(pattern)
    c = 0
    for info, value in data:
        ret.append((info, value, pattern[c]))
        c = (c + 1) % l
    return ret


def hcolor(data, thresholds):
    """ Multicolor a graph according to thresholds

    :param data: the data
    :type data: list of tuples (info, value)
    :param thresholds: dict of thresholds, format
      {<threshold>: <color>,}
    :type thresholds: dict
    :return: the colored graph
    :rtype: list of arrays
    """
    ret = []
    for info, value in data:
        newval = []
        minover = None
        maxt = 0
        for t in thresholds:
            if maxt < t:
                maxt = t
            if value > t:
                newval.append((t, thresholds[t]))
            else:
                if minover is None or minover > t:
                    minover = t
        if minover is None:
            minover = maxt

        newval.append((value, thresholds[minover]))
        ret.append((info, newval))
    return ret
